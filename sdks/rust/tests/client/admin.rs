use super::*;

#[tokio::test]
async fn api_keys_are_sent_and_scopes_enforced() {
    let server = start("auth", &["adminadminadmin1", "publish:publishpublish12"]).await;
    let anonymous = Client::new(&server.url).unwrap();
    assert_eq!(
        anonymous.queues_list().await.unwrap_err().kind(),
        "unauthorized"
    );

    let admin = Client::new(&server.url)
        .unwrap()
        .with_api_key("adminadminadmin1");
    admin
        .queues_create("emails", &QueueSettingsPatch::default())
        .await
        .unwrap();
    let publisher = Client::new(&server.url)
        .unwrap()
        .with_api_key("publishpublish12");
    publisher
        .publish("emails", &NewMessage::new(1))
        .await
        .unwrap();
    let err = publisher
        .consume("emails", &ConsumeOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), "forbidden");
    server.stop().await;
}

#[tokio::test]
async fn backups_download_to_a_file() {
    let server = start("backup", &[]).await;
    let client = Client::new(&server.url).unwrap();
    client
        .queues_create("jobs", &QueueSettingsPatch::default())
        .await
        .unwrap();
    let path = server.dir.with_extension("backup.redb");
    let _ = std::fs::remove_file(&path);
    let written = client.backup(&path).await.unwrap();
    assert!(written > 0);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), written);
    let _ = std::fs::remove_file(&path);
    server.stop().await;
}

#[tokio::test]
async fn a_backup_cut_off_midway_leaves_no_file() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let cut_off = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", cut_off.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut stream, _) = cut_off.accept().await.unwrap();
        let _ = stream.read(&mut [0; 4096]).await;
        let head = "HTTP/1.1 200 OK\r\ncontent-length: 1000000\r\n\r\n";
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(&[0; 1000]).await.unwrap();
    });
    let dir = std::env::temp_dir().join(format!("berthq-sdk-backup-cut-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("backup.redb");

    assert!(Client::new(&url).unwrap().backup(&path).await.is_err());
    let left: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
    assert!(left.is_empty(), "{left:?}");

    let server = start("backup-again", &[]).await;
    let client = Client::new(&server.url).unwrap();
    let written = client.backup(&path).await.unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), written);
    assert!(
        client.backup(&path).await.is_err(),
        "an existing file is kept"
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), written);
    let _ = std::fs::remove_dir_all(&dir);
    server.stop().await;
}
