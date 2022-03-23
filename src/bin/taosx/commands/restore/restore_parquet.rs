use glob::glob;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::Field;
use std::path::PathBuf;
use taos::Taos;

pub async fn get_parquet_files(db: &str) -> Vec<PathBuf> {
    let mut file_list = vec![];
    let paths = glob(format!("./{}/*.parquet", db).as_str()).unwrap();
    for entry in paths {
        match entry {
            Ok(path) => file_list.push(path),
            Err(e) => println!("{:?}", e),
        }
    }
    file_list
}

pub async fn restore_parquet(
    taos: &Taos,
    db: &str,
    file_list: &Vec<PathBuf>,
    start: u32,
    end: u32,
) {
    let mut index = 0;
    for file in file_list {
        if index < start || index >= end {
            index += 1;
            continue;
        }
        index += 1;
        let tb = file.to_str().unwrap().split(".").next().unwrap();
        dbg!(tb);
        let parquet_reader =
            SerializedFileReader::try_from(format!("./{}/{}.parquet", db, tb)).unwrap();
        let mut sql = format!("insert into {} values", tb);
        for row in parquet_reader {
            sql += "(";
            let mut count = 0;
            for (_, col) in row.get_column_iter() {
                if count != 0 {
                    sql += ",";
                }
                count += 1;
                match col {
                    Field::Null => todo!(),
                    Field::Bool(v) => sql += &v.to_string(),
                    Field::Byte(v) => sql += &v.to_string(),
                    Field::Short(v) => sql += &v.to_string(),
                    Field::Int(v) => sql += &v.to_string(),
                    Field::Long(v) => sql += &v.to_string(),
                    Field::UByte(v) => sql += &v.to_string(),
                    Field::UShort(v) => sql += &v.to_string(),
                    Field::UInt(v) => sql += &v.to_string(),
                    Field::ULong(v) => sql += &v.to_string(),
                    Field::Float(v) => sql += &v.to_string(),
                    Field::Double(v) => sql += &v.to_string(),
                    Field::Str(v) => sql += format!("\'{}\'", &v).as_str(),
                    Field::Bytes(v) => {
                        sql += format!("\'{}\'", std::str::from_utf8(v.data()).unwrap()).as_str()
                    }
                    Field::TimestampMillis(v) => sql += &v.to_string(),
                    Field::TimestampMicros(v) => sql += &v.to_string(),
                    _ => unreachable!(),
                }
            }
            sql += ")";
        }
        dbg!(&sql);
        taos.query(sql.as_str()).await.unwrap();
    }
}
