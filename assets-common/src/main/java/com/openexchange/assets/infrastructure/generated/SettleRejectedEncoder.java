/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ORIGIN to the feed session + MUST alert: a Settle/SettleBatch entry failed pre-validation (unknown market, non-positive price/quantity, notional overflow, bad party). Nothing mutated, consumePosition NOT advanced; the retention gate (S3) closes order entry on BRIDGE_HALT.
 */
@SuppressWarnings("all")
public final class SettleRejectedEncoder
{
    public static final int BLOCK_LENGTH = 29;
    public static final int TEMPLATE_ID = 114;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final SettleRejectedEncoder parentMessage = this;
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

    public SettleRejectedEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public SettleRejectedEncoder wrapAndApplyHeader(
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

    public static int tradeIdId()
    {
        return 1;
    }

    public static int tradeIdSinceVersion()
    {
        return 0;
    }

    public static int tradeIdEncodingOffset()
    {
        return 0;
    }

    public static int tradeIdEncodingLength()
    {
        return 8;
    }

    public static String tradeIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long tradeIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long tradeIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long tradeIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public SettleRejectedEncoder tradeId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
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
        return 8;
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

    public SettleRejectedEncoder marketId(final int value)
    {
        buffer.putInt(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int journalPositionId()
    {
        return 3;
    }

    public static int journalPositionSinceVersion()
    {
        return 0;
    }

    public static int journalPositionEncodingOffset()
    {
        return 12;
    }

    public static int journalPositionEncodingLength()
    {
        return 8;
    }

    public static String journalPositionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long journalPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long journalPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long journalPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public SettleRejectedEncoder journalPosition(final long value)
    {
        buffer.putLong(offset + 12, value, BYTE_ORDER);
        return this;
    }


    public static int reasonId()
    {
        return 4;
    }

    public static int reasonSinceVersion()
    {
        return 0;
    }

    public static int reasonEncodingOffset()
    {
        return 20;
    }

    public static int reasonEncodingLength()
    {
        return 1;
    }

    public static String reasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public SettleRejectedEncoder reason(final SettleRejectReason value)
    {
        buffer.putByte(offset + 20, (byte)value.value());
        return this;
    }

    public static int detailId()
    {
        return 5;
    }

    public static int detailSinceVersion()
    {
        return 0;
    }

    public static int detailEncodingOffset()
    {
        return 21;
    }

    public static int detailEncodingLength()
    {
        return 8;
    }

    public static String detailMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long detailNullValue()
    {
        return -9223372036854775808L;
    }

    public static long detailMinValue()
    {
        return -9223372036854775807L;
    }

    public static long detailMaxValue()
    {
        return 9223372036854775807L;
    }

    public SettleRejectedEncoder detail(final long value)
    {
        buffer.putLong(offset + 21, value, BYTE_ORDER);
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

        final SettleRejectedDecoder decoder = new SettleRejectedDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
