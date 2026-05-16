package com.taosdata.jdbc.ws.tmq.meta;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AlterTypeDeserializerTest {
  private AlterTypeDeserializer alterTypeDeserializer;

  @Mock private JsonParser jsonParser;

  @Mock private DeserializationContext deserializationContext;

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
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
            });
    assertEquals("Invalid alter type: 999", exception.getMessage());

    // Test negative invalid value
    when(jsonParser.getIntValue()).thenReturn(-1);
    exception =
        assertThrows(
            IOException.class,
            () -> {
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
    when(jsonParser.getIntValue()).thenReturn(20);
    result = alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
    assertEquals(AlterType.ALTER_STABLE_TAG_WITH_FILTER, result);
  }

  @Test
  public void testDeserializeNewAlterTypes() throws IOException {
    when(jsonParser.getIntValue()).thenReturn(11);
    assertEquals(
        AlterType.ADD_TAG_INDEX,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(13);
    assertEquals(
        AlterType.UPDATE_COLUMN_COMPRESS,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(14);
    assertEquals(
        AlterType.ADD_COLUMN_WITH_COMPRESS,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(15);
    assertEquals(
        AlterType.SET_MULTI_TAG,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(16);
    assertEquals(
        AlterType.ALTER_COLUMN_REF,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(17);
    assertEquals(
        AlterType.SET_REF_NULL,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(18);
    assertEquals(
        AlterType.ADD_COLUMN_WITH_REF,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(19);
    assertEquals(
        AlterType.ALTER_MULTI_TABLE_TAG,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));

    when(jsonParser.getIntValue()).thenReturn(20);
    assertEquals(
        AlterType.ALTER_STABLE_TAG_WITH_FILTER,
        alterTypeDeserializer.deserialize(jsonParser, deserializationContext));
  }

  @Test
  public void testDeserializeGapValue() throws IOException {
    // Value 12 is not defined
    when(jsonParser.getIntValue()).thenReturn(12);
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              alterTypeDeserializer.deserialize(jsonParser, deserializationContext);
            });
    assertEquals("Invalid alter type: 12", exception.getMessage());
  }
}
