package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class AlterTypeDeserializerTest {
    private AlterTypeDeserializer alterTypeDeserializer;

    @Mock
    private JsonParser jsonParser;

    @Mock
    private DeserializationContext deserializationContext;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        alterTypeDeserializer = new AlterTypeDeserializer();
    }

    @Test
    public void testDeserializeValidAlterType() throws IOException {
        // Test deserialization for valid alter type values
        when(jsonParser.getIntValue()).thenReturn(1);
        AlterType result = alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        assertEquals(AlterType.ADD_TAG, result);

        when(jsonParser.getIntValue()).thenReturn(2);
        result = alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        assertEquals(AlterType.DROP_TAG, result);

        when(jsonParser.getIntValue()).thenReturn(3);
        result = alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        assertEquals(AlterType.RENAME_TAG_NAME, result);
    }

    @Test
    public void testDeserializeInvalidAlterType() throws IOException {
        // Test deserialization for invalid alter type value throws IOException
        when(jsonParser.getIntValue()).thenReturn(999);
        IOException exception = assertThrows(IOException.class, () -> {
            alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        });
        assertEquals("Invalid alter type: 999", exception.getMessage());

        // Test negative invalid value
        when(jsonParser.getIntValue()).thenReturn(-1);
        exception = assertThrows(IOException.class, () -> {
            alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        });
        assertEquals("Invalid alter type: -1", exception.getMessage());
    }

    @Test
    public void testDeserializeBoundaryValues() throws IOException {
        // Test minimum valid value
        when(jsonParser.getIntValue()).thenReturn(1);
        AlterType result = alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        assertEquals(AlterType.ADD_TAG, result);

        // Test maximum valid value
        when(jsonParser.getIntValue()).thenReturn(3);
        result = alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
        assertEquals(AlterType.RENAME_TAG_NAME, result);
    }
}