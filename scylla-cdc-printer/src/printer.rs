use std::sync::Arc;
use std::time::SystemTime;

use anyhow;
use chrono::Duration;
use core::time;
use scylla::Session;
use scylla::frame::response::result::Row;
use tokio::time::sleep;

use scylla_cdc::cdc_types::GenerationTimestamp;
use scylla_cdc::reader::StreamReader;
use scylla_cdc::stream_generations::GenerationFetcher;

struct CDCLogPrinter {
    session: Arc<Session>,
    start_timestamp: Duration,
    end_timestamp: Duration,
    sleep_interval: time::Duration,
    window_size: Duration,
    safety_interval: Duration,
    readers: Vec<Arc<StreamReader>>,
}

impl CDCLogPrinter {
    fn new(
        session: &Arc<Session>,
        start_timestamp: Duration,
        end_timestamp: Duration,
        window_size: Duration,
        safety_interval: Duration,
        sleep_interval: time::Duration,
    ) -> CDCLogPrinter {
        CDCLogPrinter {
            session: Arc::clone(session),
            start_timestamp,
            end_timestamp,
            window_size,
            safety_interval,
            sleep_interval,
            readers: vec![],
        }
    }

    pub async fn run(
        &mut self,
        keyspace: String,
        table_name: String,
        mut after_fetch_callback: impl FnMut(Row),
    ) -> anyhow::Result<()> {
        let fetcher = &GenerationFetcher::new(&self.session);
        let mut generation = self.get_first_generation(fetcher).await?;

        loop {
            self.readers = vec![];
            for stream_id in fetcher
                .fetch_stream_ids(&generation)
                .await?
                .iter()
                .flatten()
            {
                let reader = StreamReader::new(
                    &self.session,
                    vec![stream_id.clone()],
                    self.start_timestamp,
                    self.window_size,
                    self.safety_interval,
                    self.sleep_interval,
                );
                self.readers.push(Arc::new(reader));
            }

            let handles = self.readers.iter().map(|reader| {
                tokio::spawn(async move {
                    reader
                        .fetch_cdc(keyspace.clone(), table_name.clone(), after_fetch_callback)
                        .await
                })
            });

            generation = loop {
                match fetcher.fetch_next_generation(&generation).await? {
                    Some(generation) => break generation,
                    None => {
                        let now = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap();
                        if now + self.sleep_interval >= self.end_timestamp.to_std()? {
                            break GenerationTimestamp { timestamp: self.end_timestamp };
                        }
                        sleep(self.sleep_interval).await
                    }
                }
            };

            self.set_upper_timestamp(generation.timestamp).await;

            futures::future::try_join_all(handles).await?;

            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            if now + self.sleep_interval >= self.end_timestamp.to_std()? {
                return Ok(())
            }
        }
    }

    async fn set_upper_timestamp(&self, new_upper_timestamp: Duration) {
        for reader in self.readers.iter() {
            (&reader).set_upper_timestamp(new_upper_timestamp).await;
        }
    }

    async fn get_first_generation(
        &self,
        fetcher: &GenerationFetcher,
    ) -> anyhow::Result<GenerationTimestamp> {
        Ok(fetcher
            .fetch_generation_by_timestamp(&self.start_timestamp)
            .await?
            .unwrap_or(loop {
                match fetcher.fetch_all_generations().await?.last() {
                    None => sleep(self.sleep_interval).await,
                    Some(generation) => break generation.clone(),
                }
            }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use scylla::batch::Consistency;
    use scylla::query::Query;
    use scylla::SessionBuilder;

    use super::*;

    const SECOND_IN_MICRO: i64 = 1_000_000;
    const SECOND_IN_MILLIS: i64 = 1_000;
    const TEST_KEYSPACE: &str = "test";
    const TEST_TABLE: &str = "t";
    const SLEEP_INTERVAL: i64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: i64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: i64 = SECOND_IN_MILLIS / 10;
    const START_TIME_DELAY_IN_MILLIS: i64 = 2 * SECOND_IN_MILLIS;

    fn get_create_table_query() -> String {
        format!("CREATE TABLE IF NOT EXISTS {}.{} (pk int, t int, v text, s text, PRIMARY KEY (pk, t)) WITH cdc = {{'enabled':true}};",
                TEST_KEYSPACE,
                TEST_TABLE
        )
    }

    async fn create_test_db(session: &Arc<Session>) -> anyhow::Result<()> {
        let mut create_keyspace_query = Query::new(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};",
            TEST_KEYSPACE
        ));
        create_keyspace_query.set_consistency(Consistency::All);

        session.query(create_keyspace_query, &[]).await?;
        session.await_schema_agreement().await?;

        // Create test table
        let create_table_query = get_create_table_query();
        session.query(create_table_query, &[]).await?;
        session.await_schema_agreement().await?;

        let table_name_under_keyspace = format!("{}.{}", TEST_KEYSPACE, TEST_TABLE);
        session
            .query(format!("TRUNCATE {};", table_name_under_keyspace), &[])
            .await?;
        session
            .query(
                format!("TRUNCATE {}_scylla_cdc_log;", table_name_under_keyspace),
                &[],
            )
            .await?;
        Ok(())
    }

    async fn populate_db_with_pk(session: &Arc<Session>, pk: u32) -> anyhow::Result<()> {
        let table_name_under_keyspace = format!("{}.{}", TEST_KEYSPACE, TEST_TABLE);
        for i in 0..3 {
            session
                .query(
                    format!(
                        "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, 'val{}', 'static{}');",
                        table_name_under_keyspace, pk, i, i, i
                    ),
                    &[],
                )
                .await?;
        }
        session.await_schema_agreement().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cdc_log_printer() {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
        let shared_session = Arc::new(session);

        let partition_key_1 = 0;
        let partition_key_2 = 1;
        create_test_db(&shared_session).await.unwrap();
        populate_db_with_pk(&shared_session, partition_key_1)
            .await
            .unwrap();
        populate_db_with_pk(&shared_session, partition_key_2).await;

        let start: Duration = Duration::from_std(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        )
            .unwrap();
        let end = start + Duration::seconds(2);
        let mut cdc_log_printer = CDCLogPrinter::new(
            &shared_session,
            start,
            end,
            Duration::milliseconds(WINDOW_SIZE),
            Duration::milliseconds(SAFETY_INTERVAL),
            time::Duration::from_millis(SLEEP_INTERVAL as u64),
        );

        let mut fetched_results: Arc<Mutex<Vec<Row>>> = Arc::new(Mutex::new(Vec::new()));
        let mut fetched_results_clone = Arc::clone(&fetched_results);
        let fetch_callback = move |row: Row| {
            match fetched_results_clone.as_ref().try_lock() {
                Ok(mut fetched_results) => fetched_results.push(row),
                Err(_) => {}
            }
        };

        cdc_log_printer
            .run(TEST_KEYSPACE.to_string(), TEST_TABLE.to_string(), fetch_callback)
            .await;
        assert_eq!(fetched_results.as_ref().lock().unwrap().len(), 3);
    }
}
