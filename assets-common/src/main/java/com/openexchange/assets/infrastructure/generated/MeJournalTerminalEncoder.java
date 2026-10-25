/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ME journal terminal (status 2/3/4 only), with the status and marketId the v5 bridge dropped. Hold-scoped residual release + the order-state transition.
 */
@SuppressWarnings("all")
public final class MeJournalTerminalEncoder
{
    public static final int BLOCK_LENGTH = 53;
    public static final int TEMPLATE_ID = 63;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeJournalTerminalEncoder parentMessage = this;
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

    public MeJournalTerminalEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public MeJournalTerminalEncoder wrapAndApplyHeader(
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

    public static int egressSeqId()
    {
        return 1;
    }

    public static int egressSeqSinceVersion()
    {
        return 0;
    }

    public static int egressSeqEncodingOffset()
    {
        return 0;
    }

    public static int egressSeqEncodingLength()
    {
        return 8;
    }

    public static String egressSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long egressSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long egressSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long egressSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTerminalEncoder egressSeq(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int orderIdId()
    {
        return 2;
    }

    public static int orderIdSinceVersion()
    {
        return 0;
    }

    public static int orderIdEncodingOffset()
    {
        return 8;
    }

    public static int orderIdEncodingLength()
    {
        return 8;
    }

    public static String orderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long orderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long orderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long orderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTerminalEncoder orderId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int userIdId()
    {
        return 3;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 16;
    }

    public static int userIdEncodingLength()
    {
        return 8;
    }

    public static String userIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long userIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long userIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long userIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTerminalEncoder userId(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int marketIdId()
    {
        return 4;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 24;
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

    public MeJournalTerminalEncoder marketId(final int value)
    {
        buffer.putInt(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int statusId()
    {
        return 5;
    }

    public static int statusSinceVersion()
    {
        return 0;
    }

    public static int statusEncodingOffset()
    {
        return 28;
    }

    public static int statusEncodingLength()
    {
        return 1;
    }

    public static String statusMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public MeJournalTerminalEncoder status(final OrderStatus value)
    {
        buffer.putByte(offset + 28, (byte)value.value());
        return this;
    }

    public static int meTimestampId()
    {
        return 6;
    }

    public static int meTimestampSinceVersion()
    {
        return 0;
    }

    public static int meTimestampEncodingOffset()
    {
        return 29;
    }

    public static int meTimestampEncodingLength()
    {
        return 8;
    }

    public static String meTimestampMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTerminalEncoder meTimestamp(final long value)
    {
        buffer.putLong(offset + 29, value, BYTE_ORDER);
        return this;
    }


    public static int omsOrderIdId()
    {
        return 7;
    }

    public static int omsOrderIdSinceVersion()
    {
        return 0;
    }

    public static int omsOrderIdEncodingOffset()
    {
        return 37;
    }

    public static int omsOrderIdEncodingLength()
    {
        return 8;
    }

    public static String omsOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long omsOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long omsOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long omsOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTerminalEncoder omsOrderId(final long value)
    {
        buffer.putLong(offset + 37, value, BYTE_ORDER);
        return this;
    }


    public static int meEpochId()
    {
        return 8;
    }

    public static int meEpochSinceVersion()
    {
        return 0;
    }

    public static int meEpochEncodingOffset()
    {
        return 45;
    }

    public static int meEpochEncodingLength()
    {
        return 8;
    }

    public static String meEpochMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meEpochNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meEpochMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meEpochMaxValue()
    {
        return 9223372036854775807L;
    }

    public MeJournalTerminalEncoder meEpoch(final long value)
    {
        buffer.putLong(offset + 45, value, BYTE_ORDER);
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

        final MeJournalTerminalDecoder decoder = new MeJournalTerminalDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
