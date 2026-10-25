/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ORIGIN reply to CreateOrder/CancelOrder/AmendOrder. duplicate = replayed requestId decision. eventSeq joins the ack to its OrderEvent.
 */
@SuppressWarnings("all")
public final class OrderAckEncoder
{
    public static final int BLOCK_LENGTH = 74;
    public static final int TEMPLATE_ID = 100;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderAckEncoder parentMessage = this;
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

    public OrderAckEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public OrderAckEncoder wrapAndApplyHeader(
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

    public static int correlationIdId()
    {
        return 1;
    }

    public static int correlationIdSinceVersion()
    {
        return 0;
    }

    public static int correlationIdEncodingOffset()
    {
        return 0;
    }

    public static int correlationIdEncodingLength()
    {
        return 8;
    }

    public static String correlationIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long correlationIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long correlationIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long correlationIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderAckEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int requestIdHighId()
    {
        return 2;
    }

    public static int requestIdHighSinceVersion()
    {
        return 0;
    }

    public static int requestIdHighEncodingOffset()
    {
        return 8;
    }

    public static int requestIdHighEncodingLength()
    {
        return 8;
    }

    public static String requestIdHighMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long requestIdHighNullValue()
    {
        return -9223372036854775808L;
    }

    public static long requestIdHighMinValue()
    {
        return -9223372036854775807L;
    }

    public static long requestIdHighMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderAckEncoder requestIdHigh(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int requestIdLowId()
    {
        return 3;
    }

    public static int requestIdLowSinceVersion()
    {
        return 0;
    }

    public static int requestIdLowEncodingOffset()
    {
        return 16;
    }

    public static int requestIdLowEncodingLength()
    {
        return 8;
    }

    public static String requestIdLowMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long requestIdLowNullValue()
    {
        return -9223372036854775808L;
    }

    public static long requestIdLowMinValue()
    {
        return -9223372036854775807L;
    }

    public static long requestIdLowMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderAckEncoder requestIdLow(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int orderIdId()
    {
        return 4;
    }

    public static int orderIdSinceVersion()
    {
        return 0;
    }

    public static int orderIdEncodingOffset()
    {
        return 24;
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

    public OrderAckEncoder orderId(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int userIdId()
    {
        return 5;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 32;
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

    public OrderAckEncoder userId(final long value)
    {
        buffer.putLong(offset + 32, value, BYTE_ORDER);
        return this;
    }


    public static int marketIdId()
    {
        return 6;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 40;
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

    public OrderAckEncoder marketId(final int value)
    {
        buffer.putInt(offset + 40, value, BYTE_ORDER);
        return this;
    }


    public static int statusId()
    {
        return 7;
    }

    public static int statusSinceVersion()
    {
        return 0;
    }

    public static int statusEncodingOffset()
    {
        return 44;
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

    public OrderAckEncoder status(final OrderStatus value)
    {
        buffer.putByte(offset + 44, (byte)value.value());
        return this;
    }

    public static int duplicateId()
    {
        return 8;
    }

    public static int duplicateSinceVersion()
    {
        return 0;
    }

    public static int duplicateEncodingOffset()
    {
        return 45;
    }

    public static int duplicateEncodingLength()
    {
        return 1;
    }

    public static String duplicateMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public OrderAckEncoder duplicate(final BoolFlag value)
    {
        buffer.putByte(offset + 45, (byte)value.value());
        return this;
    }

    public static int holdAssetIdId()
    {
        return 9;
    }

    public static int holdAssetIdSinceVersion()
    {
        return 0;
    }

    public static int holdAssetIdEncodingOffset()
    {
        return 46;
    }

    public static int holdAssetIdEncodingLength()
    {
        return 4;
    }

    public static String holdAssetIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int holdAssetIdNullValue()
    {
        return -2147483648;
    }

    public static int holdAssetIdMinValue()
    {
        return -2147483647;
    }

    public static int holdAssetIdMaxValue()
    {
        return 2147483647;
    }

    public OrderAckEncoder holdAssetId(final int value)
    {
        buffer.putInt(offset + 46, value, BYTE_ORDER);
        return this;
    }


    public static int holdAmountId()
    {
        return 10;
    }

    public static int holdAmountSinceVersion()
    {
        return 0;
    }

    public static int holdAmountEncodingOffset()
    {
        return 50;
    }

    public static int holdAmountEncodingLength()
    {
        return 8;
    }

    public static String holdAmountMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long holdAmountNullValue()
    {
        return -9223372036854775808L;
    }

    public static long holdAmountMinValue()
    {
        return -9223372036854775807L;
    }

    public static long holdAmountMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderAckEncoder holdAmount(final long value)
    {
        buffer.putLong(offset + 50, value, BYTE_ORDER);
        return this;
    }


    public static int acceptedAtMsId()
    {
        return 11;
    }

    public static int acceptedAtMsSinceVersion()
    {
        return 0;
    }

    public static int acceptedAtMsEncodingOffset()
    {
        return 58;
    }

    public static int acceptedAtMsEncodingLength()
    {
        return 8;
    }

    public static String acceptedAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long acceptedAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long acceptedAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long acceptedAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderAckEncoder acceptedAtMs(final long value)
    {
        buffer.putLong(offset + 58, value, BYTE_ORDER);
        return this;
    }


    public static int eventSeqId()
    {
        return 12;
    }

    public static int eventSeqSinceVersion()
    {
        return 0;
    }

    public static int eventSeqEncodingOffset()
    {
        return 66;
    }

    public static int eventSeqEncodingLength()
    {
        return 8;
    }

    public static String eventSeqMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long eventSeqNullValue()
    {
        return -9223372036854775808L;
    }

    public static long eventSeqMinValue()
    {
        return -9223372036854775807L;
    }

    public static long eventSeqMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderAckEncoder eventSeq(final long value)
    {
        buffer.putLong(offset + 66, value, BYTE_ORDER);
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

        final OrderAckDecoder decoder = new OrderAckDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
