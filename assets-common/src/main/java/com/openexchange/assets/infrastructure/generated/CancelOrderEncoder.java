/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Cancel by core orderId (or by origClientOrderId when orderId = 0). Owner check: order.userId == userId else NOT_OWNER.
 */
@SuppressWarnings("all")
public final class CancelOrderEncoder
{
    public static final int BLOCK_LENGTH = 56;
    public static final int TEMPLATE_ID = 51;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CancelOrderEncoder parentMessage = this;
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

    public CancelOrderEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public CancelOrderEncoder wrapAndApplyHeader(
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

    public CancelOrderEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int userIdId()
    {
        return 2;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 8;
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

    public CancelOrderEncoder userId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
    }


    public static int orderIdId()
    {
        return 3;
    }

    public static int orderIdSinceVersion()
    {
        return 0;
    }

    public static int orderIdEncodingOffset()
    {
        return 16;
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

    public CancelOrderEncoder orderId(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int requestIdHighId()
    {
        return 4;
    }

    public static int requestIdHighSinceVersion()
    {
        return 0;
    }

    public static int requestIdHighEncodingOffset()
    {
        return 24;
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

    public CancelOrderEncoder requestIdHigh(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int requestIdLowId()
    {
        return 5;
    }

    public static int requestIdLowSinceVersion()
    {
        return 0;
    }

    public static int requestIdLowEncodingOffset()
    {
        return 32;
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

    public CancelOrderEncoder requestIdLow(final long value)
    {
        buffer.putLong(offset + 32, value, BYTE_ORDER);
        return this;
    }


    public static int origClientOrderIdHighId()
    {
        return 6;
    }

    public static int origClientOrderIdHighSinceVersion()
    {
        return 0;
    }

    public static int origClientOrderIdHighEncodingOffset()
    {
        return 40;
    }

    public static int origClientOrderIdHighEncodingLength()
    {
        return 8;
    }

    public static String origClientOrderIdHighMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long origClientOrderIdHighNullValue()
    {
        return -9223372036854775808L;
    }

    public static long origClientOrderIdHighMinValue()
    {
        return -9223372036854775807L;
    }

    public static long origClientOrderIdHighMaxValue()
    {
        return 9223372036854775807L;
    }

    public CancelOrderEncoder origClientOrderIdHigh(final long value)
    {
        buffer.putLong(offset + 40, value, BYTE_ORDER);
        return this;
    }


    public static int origClientOrderIdLowId()
    {
        return 7;
    }

    public static int origClientOrderIdLowSinceVersion()
    {
        return 0;
    }

    public static int origClientOrderIdLowEncodingOffset()
    {
        return 48;
    }

    public static int origClientOrderIdLowEncodingLength()
    {
        return 8;
    }

    public static String origClientOrderIdLowMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long origClientOrderIdLowNullValue()
    {
        return -9223372036854775808L;
    }

    public static long origClientOrderIdLowMinValue()
    {
        return -9223372036854775807L;
    }

    public static long origClientOrderIdLowMaxValue()
    {
        return 9223372036854775807L;
    }

    public CancelOrderEncoder origClientOrderIdLow(final long value)
    {
        buffer.putLong(offset + 48, value, BYTE_ORDER);
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

        final CancelOrderDecoder decoder = new CancelOrderDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
