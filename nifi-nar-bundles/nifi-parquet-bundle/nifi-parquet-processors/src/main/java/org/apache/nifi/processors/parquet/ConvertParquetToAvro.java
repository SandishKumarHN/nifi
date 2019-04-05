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
package org.apache.nifi.processors.parquet;

import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.parquet.utils.AvroUtils;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"parquet", "avro", "convert"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts Parquet records into Avro file format. The incoming FlowFile should be a valid parquet file. If an incoming FlowFile does "
        + "not contain any records, an empty avro file is the output. NOTE: Many Parquet datatypes (collections, primitives, and unions of primitives, e.g.) can "
        + "be converted to avro.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "Sets the filename to the existing filename with the extension replaced by / added to by .avro"),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the parquet file.")
})
public class ConvertParquetToAvro extends AbstractProcessor {

    // Attributes
    public static final String RECORD_COUNT_ATTRIBUTE = "record.count";

    private volatile List<PropertyDescriptor> avroProps;

    // Relationships
    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Avro file that was converted successfully from Parquet")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Parquet content that could not be processed")
            .build();

    static final Set<Relationship> RELATIONSHIPS
            = ImmutableSet.<Relationship>builder()
            .add(SUCCESS)
            .add(FAILURE)
            .build();

    @Override
    protected final void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(AvroUtils.COMPRESSION_TYPE);
        props.add(AvroUtils.SYNC_INTERVAL);
        props.add(AvroUtils.FLUSH_ON_EVERYBLOCK);
        this.avroProps = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return avroProps;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {

            long startTime = System.currentTimeMillis();
            final AtomicInteger totalRecordCount = new AtomicInteger(0);

            final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());

            FlowFile putFlowFile = flowFile;

            getLogger().info( "----------: avro schema: ---------" + putFlowFile.getAttributes());

            putFlowFile = session.write(flowFile, (rawIn, rawOut) -> {
                try (final InputStream in = new BufferedInputStream(rawIn);
                     //final DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<>(in, new GenericDatumReader<>())) {
                    DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<GenericData.Record>(in, new GenericDatumReader<GenericData.Record>())) {
                    Schema avroSchema = dataFileReader.getSchema();
                    getLogger().info("----------: avro schema: ---------" + dataFileReader.getSchema().toString());
                    getLogger().debug(avroSchema.toString(true));
                    DataFileWriter<GenericRecord> writer = createAvroWriter(context, flowFile, rawOut, avroSchema );

                    try {
                        int recordCount = 0;
                        GenericRecord rec = null;
                        while (dataFileReader.hasNext()) {
                            rec = AvroTypeUtil.createAvroRecord(dataFileReader.next(), avroSchema);
                            writer.append(rec);
                            recordCount++;
                        }
                        totalRecordCount.set(recordCount);
                    } finally {
                        writer.close();
                    }
                }
            });

            // Add attributes and transfer to success
            StringBuilder newFilename = new StringBuilder();
            int extensionIndex = fileName.lastIndexOf(".");
            if (extensionIndex != -1) {
                newFilename.append(fileName.substring(0, extensionIndex));
            } else {
                newFilename.append(fileName);
            }
            newFilename.append(".avro");

            Map<String,String> outAttributes = new HashMap<>();
            outAttributes.put(CoreAttributes.FILENAME.key(), newFilename.toString());
            outAttributes.put(RECORD_COUNT_ATTRIBUTE,Integer.toString(totalRecordCount.get()) );

            putFlowFile = session.putAllAttributes(putFlowFile, outAttributes);
            session.transfer(putFlowFile, SUCCESS);
            session.getProvenanceReporter().modifyContent(putFlowFile, "Converted "+totalRecordCount.get()+" records", System.currentTimeMillis() - startTime);

        } catch (final ProcessException pe) {
            getLogger().error("Failed to convert {} from Parquet to Avro due to {}; transferring to failure", new Object[]{flowFile, pe});
            session.transfer(flowFile, FAILURE);
        }
    }

    private DataFileWriter<GenericRecord> createAvroWriter(final ProcessContext context, final FlowFile flowFile, final OutputStream out, final Schema schema)
            throws IOException {
        final String compressionFormat = context.getProperty(AvroUtils.COMPRESSION_TYPE).getValue();
        final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(AvroUtils.getCodecFactory(compressionFormat));
        return dataFileWriter.create(schema, out);
    }
}