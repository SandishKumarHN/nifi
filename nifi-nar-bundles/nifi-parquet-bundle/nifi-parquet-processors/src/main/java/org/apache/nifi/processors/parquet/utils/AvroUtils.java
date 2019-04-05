package org.apache.nifi.processors.parquet.utils;

import org.apache.avro.file.CodecFactory;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class AvroUtils {

    public static List<AllowableValue> COMPRESSION_TYPES = getCompressionTypes();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("compression-type")
            .displayName("Compression Type")
            .description("The type of compression for the file being written.")
            .allowableValues(COMPRESSION_TYPES.toArray(new AllowableValue[0]))
            .defaultValue(COMPRESSION_TYPES.get(0).getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SYNC_INTERVAL = new PropertyDescriptor.Builder()
            .name("sync-interval")
            .displayName("Avro Sync Interval")
            .description("Set the synchronization interval for this file, in bytes.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1024")
            .required(true)
            .build();

    public static final PropertyDescriptor FLUSH_ON_EVERYBLOCK = new PropertyDescriptor.Builder()
            .name("flush-on-every-block")
            .displayName("Avro Write Flush on Every Block")
            .description("Set whether this writer should flush the block to the stream every time a sync marker is written.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static List<AllowableValue> getCompressionTypes() {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        for (CompressionCodecName compressionCodecName : CompressionCodecName.values()) {
            final String name = compressionCodecName.name();
            compressionTypes.add(new AllowableValue(name, name));
        }
        return  Collections.unmodifiableList(compressionTypes);
    }

    public static CodecFactory getCodecFactory(String property) {
        CodecType type = CodecType.valueOf(property);
        switch (type) {
            case BZIP2:
                return CodecFactory.bzip2Codec();
            case DEFLATE:
                return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
            case LZO:
                return CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
            case SNAPPY:
                return CodecFactory.snappyCodec();
            case NONE:
            default:
                return CodecFactory.nullCodec();
        }
    }

    private enum CodecType {
        BZIP2,
        DEFLATE,
        NONE,
        SNAPPY,
        LZO
    }
}