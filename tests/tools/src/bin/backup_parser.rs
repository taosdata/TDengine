use taosx_core::taoz::ZCodec;
use taosx_core::taoz::ZMessage;

/// 本地备份文件的解析器
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const PATH: &str = "./tests/tools/sample/x2a9beed6122-1757299958435-129-1.z";

    let reader = tokio::fs::File::open(PATH).await?;

    let reader = tokio::io::BufReader::new(reader);
    let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);

    // read header
    let mut reader = ZCodec::new(reader);
    let header = reader.header_async().await?;
    println!("header: {:?}", header);

    // read body
    loop {
        match reader.read_message_async().await {
            Ok(msg) => match msg {
                ZMessage::Meta(meta) => {
                    println!("meta len: {}", meta.raw_len());
                }
                ZMessage::Data(raw_blocks) => {
                    println!("raw blocks: {}", raw_blocks.len());
                }
                ZMessage::Raw(raw_type, raw_data) => {
                    println!("type: {:?}, data len: {}", raw_type, raw_data.raw_len());
                }
            },
            Err(err) => {
                // 如果是 EOF，表示文件读取完成
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!("reading file {} done", PATH);
                    break;
                }
                // 其他错误，打印错误信息
                println!("reading data error: {}", &err);
                break;
            }
        }
    }

    Ok(())
}
