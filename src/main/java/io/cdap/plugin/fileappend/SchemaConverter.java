/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.fileappend;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;

public class SchemaConverter {
  private static final ReflectionSchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();
  private final boolean escapeColumns;

  public SchemaConverter(boolean escapeColumns) {
    this.escapeColumns = escapeColumns;
  }

  /**
   * Create a hive schema from the given type using reflection.
   *
   * @param type type of class to derive the schema from.
   * @return hive schema that can be used in a create statement.
   * @throws UnsupportedTypeException if there is an unsupported type.
   */
  public String toHiveSchema(Type type) throws UnsupportedTypeException {
    // false is to disallow recursive schemas, which hive can't handle
    return toHiveSchema(schemaGenerator.generate(type, false));
  }

  /**
   * Translate the given schema into a hive schema. Assumes the input schema is not recursive.
   *
   * @param schema schema to translate.
   * @return hive schema that can be used in a create statement.
   */
  public String toHiveSchema(Schema schema) throws UnsupportedTypeException {
    if (schema.getType() != Schema.Type.RECORD || schema.getFields().size() < 1) {
      throw new UnsupportedTypeException("Schema must be of type record and have at least one field.");
    }

    StringBuilder builder = new StringBuilder();

    // schema is guaranteed to have at least one field, and all field names are guaranteed to be unique.
    Iterator<Schema.Field> fieldIter = schema.getFields().iterator();
    appendField(builder, fieldIter.next(), false);
    while (fieldIter.hasNext()) {
      builder.append(", ");
      appendField(builder, fieldIter.next(), false);
    }

    return builder.toString();
  }

  private void appendType(StringBuilder builder, Schema schema) throws UnsupportedTypeException {
    switch (schema.getType()) {
      case NULL:
        break;
      case ENUM:
        builder.append("string");
        break;
      case BOOLEAN:
        builder.append("boolean");
        break;
      case INT:
        builder.append("int");
        break;
      case LONG:
        builder.append("bigint");
        break;
      case FLOAT:
        builder.append("float");
        break;
      case DOUBLE:
        builder.append("double");
        break;
      case BYTES:
        builder.append("binary");
        break;
      case STRING:
        builder.append("string");
        break;
      case ARRAY:
        // array<string>
        builder.append("array<");
        appendType(builder, schema.getComponentSchema());
        builder.append(">");
        break;
      case MAP:
        // map<string,int>
        builder.append("map<");
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        appendType(builder, mapSchema.getKey());
        builder.append(",");
        appendType(builder, mapSchema.getValue());
        builder.append(">");
        break;
      case RECORD:
        //struct<name:string,ints:array<int>>
        builder.append("struct<");
        Iterator<Schema.Field> fieldIter = schema.getFields().iterator();
        appendField(builder, fieldIter.next(), true);
        while (fieldIter.hasNext()) {
          builder.append(",");
          appendField(builder, fieldIter.next(), true);
        }
        builder.append(">");
        break;
      case UNION:
        // if something is nullable, it is a union of null and the other type.
        if (schema.isNullable()) {
          appendType(builder, schema.getNonNullable());
        } else {
          // TODO: support hive unions
          throw new UnsupportedTypeException("Unions are currently not supported");
        }
    }
  }

  private void appendField(StringBuilder builder, Schema.Field field, boolean inStruct)
    throws UnsupportedTypeException {
    String name = field.getName();
    if (escapeColumns) {
      // a literal backtick(`) is represented as a double backtick(``)
      builder.append('`').append(name.replace("`", "``")).append('`');
    } else {
      builder.append(name);
    }
    // structs look like "struct<name1:string,name2:array<int>>"
    // outside a struct fields look like "name1 string, name2 array<int>"
    builder.append(inStruct ? ":" : " ");
    appendType(builder, field.getSchema());
  }
}
