use serde::{Deserialize, Serialize};

use clickhouse::{Client, Row};

mod common;

#[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
struct MyRow {
    num: u32,
}

async fn create_table(client: &Client) {
    client
        .query(
            "
            CREATE TABLE some(num UInt32)
            ENGINE = MergeTree
            ORDER BY num
        ",
        )
        .execute()
        .await
        .unwrap();
}

async fn insert_into_table(client: &Client, rows: &[MyRow]) {
    let mut insert = client.insert("some").unwrap();
    for row in rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();
}

#[tokio::test]
async fn it_watches_changes() {
    let client = common::prepare_database("it_watches_changes").await;

    create_table(&client).await;

    let mut cursor1 = client
        .watch("SELECT ?fields FROM some ORDER BY num")
        .limit(1)
        .fetch::<MyRow>()
        .unwrap();

    let mut cursor2 = client
        .watch("SELECT sum(num) as num FROM some")
        .fetch::<MyRow>()
        .unwrap();

    // Insert first batch.
    insert_into_table(&client, &[MyRow { num: 1 }, MyRow { num: 2 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some((1, MyRow { num: 1 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((1, MyRow { num: 2 })));
    assert_eq!(cursor2.next().await.unwrap(), Some((1, MyRow { num: 3 })));

    // Insert second batch.
    insert_into_table(&client, &[MyRow { num: 3 }, MyRow { num: 4 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 1 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 2 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 3 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 4 })));
    assert_eq!(cursor2.next().await.unwrap(), Some((2, MyRow { num: 10 })));

    // Insert third batch.
    insert_into_table(&client, &[MyRow { num: 5 }, MyRow { num: 6 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), None);
    assert_eq!(cursor2.next().await.unwrap(), Some((3, MyRow { num: 21 })));
}

#[tokio::test]
async fn it_watches_events() {
    let client = common::prepare_database("it_watches_events").await;

    create_table(&client).await;

    let mut cursor1 = client
        .watch("SELECT num FROM some ORDER BY num")
        .limit(1)
        .only_events()
        .fetch()
        .unwrap();

    let mut cursor2 = client
        .watch("SELECT sum(num) as num FROM some")
        .only_events()
        .fetch()
        .unwrap();

    // Insert first batch.
    insert_into_table(&client, &[MyRow { num: 1 }, MyRow { num: 2 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some(1));
    assert_eq!(cursor2.next().await.unwrap(), Some(1));

    // Insert second batch.
    insert_into_table(&client, &[MyRow { num: 3 }, MyRow { num: 4 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), Some(2));
    assert_eq!(cursor2.next().await.unwrap(), Some(2));

    // Insert third batch.
    insert_into_table(&client, &[MyRow { num: 5 }, MyRow { num: 6 }]).await;
    assert_eq!(cursor1.next().await.unwrap(), None);
    assert_eq!(cursor2.next().await.unwrap(), Some(3));
}
