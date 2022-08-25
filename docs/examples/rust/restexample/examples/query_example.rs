use taos::sync::*;

fn main() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("ws:///power")?.build()?;
    let mut result = taos.query("SELECT ts, current FROM meters LIMIT 2")?;
    // print column names
    let meta = result.fields();
    println!("{}", meta.iter().map(|field| field.name()).join("\t"));

    // print rows
    let rows = result.rows();
    for row in rows {
        let row = row?;
        for (_name, value) in row {
            print!("{}\t", value);
        }
        println!();
    }
    Ok(())
}

// output(suppose you are in +8 timezone):
// ts      current
// 2018-10-03T14:38:05+08:00       10.3
// 2018-10-03T14:38:15+08:00       12.6
