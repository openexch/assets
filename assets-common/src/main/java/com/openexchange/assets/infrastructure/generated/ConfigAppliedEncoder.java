/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * BROADCAST control: a config record (CORE/RISK/BREAKER/GATE) was applied at configVersion for marketId (0 = global/default).
 */
@SuppressWarnings("all")
public final class ConfigAppliedEncoder
{
    public static final int BLOCK_LENGTH = 17;
    public static final int TEMPLATE_ID = 112;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final ConfigAppliedEncoder parentMessage = this;
    private MutableDirectBuffer buffer;
    private int offset;
    private int limit;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public ConfigAppliedEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ConfigAppliedEncoder wrapAndApplyHeader(
        final MutableDirectBuffer buffer, final int offset, final MessageHeaderEncoder headerEncoder)
    {
        headerEncoder
            .wrap(buffer, offset)
            .blockLength(BLOCK_LENGTH)
            .templateId(TEMPLATE_ID)
            .schemaId(SCHEMA_ID)
            .version(SCHEMA_VERSION);

        return wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int kindId()
    {
        return 1;
    }

    public static int kindSinceVersion()
    {
        return 0;
    }

    public static int kindEncodingOffset()
    {
        return 0;
    }

    public static int kindEncodingLength()
    {
        return 1;
    }

    public static String kindMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public ConfigAppliedEncoder kind(final ConfigKind value)
    {
        buffer.putByte(offset + 0, (byte)value.value());
        return this;
    }

    public static int marketIdId()
    {
        return 2;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 1;
    }

    public static int marketIdEncodingLength()
    {
        return 4;
    }

    public static String marketIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int marketIdNullValue()
    {
        return -2147483648;
    }

    public static int marketIdMinValue()
    {
        return -2147483647;
    }

    public static int marketIdMaxValue()
    {
        return 2147483647;
    }

    public ConfigAppliedEncoder marketId(final int value)
    {
        buffer.putInt(offset + 1, value, BYTE_ORDER);
        return this;
    }


    public static int configVersionId()
    {
        return 3;
    }

    public static int configVersionSinceVersion()
    {
        return 0;
    }

    public static int configVersionEncodingOffset()
    {
        return 5;
    }

    public static int configVersionEncodingLength()
    {
        return 4;
    }

    public static String configVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long configVersionNullValue()
    {
        return 4294967295L;
    }

    public static long configVersionMinValue()
    {
        return 0L;
    }

    public static long configVersionMaxValue()
    {
        return 4294967294L;
    }

    public ConfigAppliedEncoder configVersion(final long value)
    {
        buffer.putInt(offset + 5, (int)value, BYTE_ORDER);
        return this;
    }


    public static int logPositionId()
    {
        return 4;
    }

    public static int logPositionSinceVersion()
    {
        return 0;
    }

    public static int logPositionEncodingOffset()
    {
        return 9;
    }

    public static int logPositionEncodingLength()
    {
        return 8;
    }

    public static String logPositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long logPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long logPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long logPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public ConfigAppliedEncoder logPosition(final long value)
    {
        buffer.putLong(offset + 9, value, BYTE_ORDER);
        return this;
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final ConfigAppliedDecoder decoder = new ConfigAppliedDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
