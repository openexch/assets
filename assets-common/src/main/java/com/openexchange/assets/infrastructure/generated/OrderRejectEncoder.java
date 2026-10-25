/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ORIGIN reject for every ORIGIN-replyable ingress (refTemplateId names it, incl. 1/2/3 money commands under a class/gate reject). meReason = the ME OrderRejectReason when reason = ME_REJECTED, else 255. detail = the breached limit or offending value.
 */
@SuppressWarnings("all")
public final class OrderRejectEncoder
{
    public static final int BLOCK_LENGTH = 64;
    public static final int TEMPLATE_ID = 101;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderRejectEncoder parentMessage = this;
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

    public OrderRejectEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public OrderRejectEncoder wrapAndApplyHeader(
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

    public OrderRejectEncoder correlationId(final long value)
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

    public OrderRejectEncoder requestIdHigh(final long value)
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

    public OrderRejectEncoder requestIdLow(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int userIdId()
    {
        return 4;
    }

    public static int userIdSinceVersion()
    {
        return 0;
    }

    public static int userIdEncodingOffset()
    {
        return 24;
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

    public OrderRejectEncoder userId(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int orderIdId()
    {
        return 5;
    }

    public static int orderIdSinceVersion()
    {
        return 0;
    }

    public static int orderIdEncodingOffset()
    {
        return 32;
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

    public OrderRejectEncoder orderId(final long value)
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

    public OrderRejectEncoder marketId(final int value)
    {
        buffer.putInt(offset + 40, value, BYTE_ORDER);
        return this;
    }


    public static int refTemplateIdId()
    {
        return 7;
    }

    public static int refTemplateIdSinceVersion()
    {
        return 0;
    }

    public static int refTemplateIdEncodingOffset()
    {
        return 44;
    }

    public static int refTemplateIdEncodingLength()
    {
        return 2;
    }

    public static String refTemplateIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int refTemplateIdNullValue()
    {
        return 65535;
    }

    public static int refTemplateIdMinValue()
    {
        return 0;
    }

    public static int refTemplateIdMaxValue()
    {
        return 65534;
    }

    public OrderRejectEncoder refTemplateId(final int value)
    {
        buffer.putShort(offset + 44, (short)value, BYTE_ORDER);
        return this;
    }


    public static int reasonId()
    {
        return 8;
    }

    public static int reasonSinceVersion()
    {
        return 0;
    }

    public static int reasonEncodingOffset()
    {
        return 46;
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

    public OrderRejectEncoder reason(final CoreRejectReason value)
    {
        buffer.putByte(offset + 46, (byte)value.value());
        return this;
    }

    public static int meReasonId()
    {
        return 9;
    }

    public static int meReasonSinceVersion()
    {
        return 0;
    }

    public static int meReasonEncodingOffset()
    {
        return 47;
    }

    public static int meReasonEncodingLength()
    {
        return 1;
    }

    public static String meReasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short meReasonNullValue()
    {
        return (short)255;
    }

    public static short meReasonMinValue()
    {
        return (short)0;
    }

    public static short meReasonMaxValue()
    {
        return (short)254;
    }

    public OrderRejectEncoder meReason(final short value)
    {
        buffer.putByte(offset + 47, (byte)value);
        return this;
    }


    public static int detailId()
    {
        return 10;
    }

    public static int detailSinceVersion()
    {
        return 0;
    }

    public static int detailEncodingOffset()
    {
        return 48;
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

    public OrderRejectEncoder detail(final long value)
    {
        buffer.putLong(offset + 48, value, BYTE_ORDER);
        return this;
    }


    public static int rejectedAtMsId()
    {
        return 11;
    }

    public static int rejectedAtMsSinceVersion()
    {
        return 0;
    }

    public static int rejectedAtMsEncodingOffset()
    {
        return 56;
    }

    public static int rejectedAtMsEncodingLength()
    {
        return 8;
    }

    public static String rejectedAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long rejectedAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long rejectedAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long rejectedAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderRejectEncoder rejectedAtMs(final long value)
    {
        buffer.putLong(offset + 56, value, BYTE_ORDER);
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

        final OrderRejectDecoder decoder = new OrderRejectDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
