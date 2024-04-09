/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.data;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public abstract class CatalogApi {

  private final Catalog catalog;
  private final IcebergSinkConfig config;

  CatalogApi(Catalog catalog, IcebergSinkConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  TableIdentifier tableId(String name) {
    return TableIdentifier.parse(name);
  }

  public final Table loadTable(TableIdentifier identifier) {
    return catalog.loadTable(identifier);
  }

  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return catalog.createTable(identifier, schema, spec, properties);
  }

  public Schema schema(TableIdentifier identifier, SinkRecord sample) {
    Types.StructType structType;
    if (sample.valueSchema() == null) {
      structType =
          SchemaUtils.inferIcebergType(sample.value(), config)
              .orElseThrow(() -> new DataException("Unable to create table from empty object"))
              .asStructType();
    } else {
      structType = SchemaUtils.toIcebergType(sample.valueSchema(), config).asStructType();
    }

    return new org.apache.iceberg.Schema(structType.fields());
  }

  public static class ErrorHandlingCatalogApi extends CatalogApi {

    private final TableIdentifier deadLetterTableId;
    private final Catalog catalog;

    ErrorHandlingCatalogApi(Catalog catalog, IcebergSinkConfig config) {
      super(catalog, config);
      this.deadLetterTableId = TableIdentifier.parse(config.deadLetterTableName());
      this.catalog = catalog;
    }

    @Override
    TableIdentifier tableId(String name) {
      TableIdentifier tableId;
      try {
        tableId = super.tableId(name);
      } catch (Exception error) {
        throw new DeadLetterUtils.DeadLetterException("TABLE_IDENTIFIER", error);
      }
      return tableId;
    }

    @Override
    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties) {

      Table table;
      if (identifier == deadLetterTableId) {
        table = catalog.createTable(identifier, schema, spec, properties);
      } else {
        try {
          table = catalog.createTable(identifier, schema, spec, properties);
        } catch (IllegalArgumentException | ValidationException error) {
          throw new DeadLetterUtils.DeadLetterException("CREATE_TABLE", error);
        }
      }
      return table;
    }

    @Override
    public Schema schema(TableIdentifier identifier, SinkRecord sample) {
      Schema schema;
      if (identifier == deadLetterTableId) {
        schema = super.schema(identifier, sample);
      } else {
        try {
          schema = super.schema(identifier, sample);
        } catch (IllegalArgumentException | ValidationException error) {
          throw new DeadLetterUtils.DeadLetterException("CREATE_SCHEMA", error);
        }
      }
      return schema;
    }
  }
}
