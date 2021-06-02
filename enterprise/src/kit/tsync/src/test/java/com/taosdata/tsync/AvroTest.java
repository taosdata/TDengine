package com.taosdata.tsync;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroTest {
    private static final String schemaStr = "{\n" +
            "  \"namespace\": \"com.taosdata.tsync.domain\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"User\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"favorite_number\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"favorite_color\",\n" +
            "      \"type\": [\n" +
            "        \"string\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Test
    public void avroWithoutCodeGeneration() throws IOException {
        Schema schema = new Schema.Parser().parse(schemaStr);
        // user 1
        GenericData.Record user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        // user 2
        GenericData.Record user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        // serializing
        DatumWriter datumWriter = new GenericDatumWriter(schema);
        DataFileWriter dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
        // deserializing
        DatumReader datumReader = new GenericDatumReader(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File("users.avro"), datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

}
