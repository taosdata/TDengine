package com.taosdata.tsync.serializer;

import com.taosdata.tsync.utils.SchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

public class TQueueAvroSerializer<T> implements Serializer<T> {

    private static byte[] serialize(Schema schema, List<GenericRecord> records, byte[] schemaBytes) {
        byte[] result = null;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] sync = new byte[16];
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            for (int i = 0; i < records.size(); i++) {
                //write records
                writer.write(records.get(i), encoder);
            }
            encoder.flush();
            byte[] value = out.toByteArray();
            int blockSize = value.length;
            out.reset();
            encoder.writeFixed(DataFileConstants.MAGIC);
            //map start
            encoder.writeMapStart();

            encoder.setItemCount(1);
            encoder.startItem();

            encoder.writeString("avro.schema");
            encoder.writeBytes(schemaBytes);
            encoder.writeMapEnd();

            encoder.writeFixed(sync);
            encoder.flush();
            out.flush();
            result = out.toByteArray();
            out.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public byte[] serialize(T data) {
        Field[] declaredFields = data.getClass().getDeclaredFields();
        for (Field field : declaredFields) {
            if (!field.isAccessible())
                continue;
        }

        String schema = SchemaGenerator.build(data.getClass());

        return new byte[0];
    }
}
