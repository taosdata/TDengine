#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum MessageType {
    NBirth,
    NDeath,
    DBirth,
    DDeath,
    NData,
    DData,
    NCmd,
    DCmd,
    State,
}

impl TryFrom<faststr::FastStr> for MessageType {
    type Error = anyhow::Error;

    fn try_from(value: faststr::FastStr) -> anyhow::Result<Self> {
        value.as_str().parse()
    }
}

impl std::str::FromStr for MessageType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            "NBIRTH" => Self::NBirth,
            "NDEATH" => Self::NDeath,
            "DBIRTH" => Self::DBirth,
            "DDEATH" => Self::DDeath,
            "NDATA" => Self::NData,
            "DDATA" => Self::DData,
            "NCMD" => Self::NCmd,
            "DCMD" => Self::DCmd,
            "STATE" => Self::State,
            s => anyhow::bail!("unsupported message type: {s}"),
        })
    }
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MessageType::NBirth => "NBIRTH",
                MessageType::NDeath => "NDEATH",
                MessageType::DBirth => "DBIRTH",
                MessageType::DDeath => "DDEATH",
                MessageType::NData => "NDATA",
                MessageType::DData => "DDATA",
                MessageType::NCmd => "NCMD",
                MessageType::DCmd => "DCMD",
                MessageType::State => "STATE",
            }
        )
    }
}
