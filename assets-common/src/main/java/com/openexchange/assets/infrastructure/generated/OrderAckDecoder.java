/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * ORIGIN reply to CreateOrder/CancelOrder/AmendOrder. duplicate = replayed requestId decision. eventSeq joins the ack to its OrderEvent.
 */
@SuppressWarnings("all")
public final class OrderAckDecoder
{
    public static final int BLOCK_LENGTH = 74;
    public static final int TEMPLATE_ID = 100;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderAckDecoder parentMessage = this;
    private DirectBuffer buffer;
    private int offset;
    private int limit;
    int actingBlockLength;
    int actingVersion;

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

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public OrderAckDecoder wrap(
        final DirectBuffer buffer,
        final int offset,
        final int actingBlockLength,
        final int actingVersion)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public OrderAckDecoder wrapAndApplyHeader(
        final DirectBuffer buffer,
        final int offset,
        final MessageHeaderDecoder headerDecoder)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        if (TEMPLATE_ID != templateId)
        {
            throw new IllegalStateException("Invalid TEMPLATE_ID: " + templateId);
        }

        return wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
    }

    public OrderAckDecoder sbeRewind()
    {
        return wrap(buffer, offset, actingBlockLength, actingVersion);
    }

    public int sbeDecodedLength()
    {
        final int currentLimit = limit();
        sbeSkip();
        final int decodedLength = encodedLength();
        limit(currentLimit);

        return decodedLength;
    }

    public int actingVersion()
    {
        return actingVersion;
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

    public long correlationId()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
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

    public long requestIdHigh()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
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

    public long requestIdLow()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
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

    public long orderId()
    {
        return buffer.getLong(offset + 24, BYTE_ORDER);
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

    public long userId()
    {
        return buffer.getLong(offset + 32, BYTE_ORDER);
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

    public int marketId()
    {
        return buffer.getInt(offset + 40, BYTE_ORDER);
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

    public short statusRaw()
    {
        return ((short)(buffer.getByte(offset + 44) & 0xFF));
    }

    public OrderStatus status()
    {
        return OrderStatus.get(((short)(buffer.getByte(offset + 44) & 0xFF)));
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

    public short duplicateRaw()
    {
        return ((short)(buffer.getByte(offset + 45) & 0xFF));
    }

    public BoolFlag duplicate()
    {
        return BoolFlag.get(((short)(buffer.getByte(offset + 45) & 0xFF)));
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

    public int holdAssetId()
    {
        return buffer.getInt(offset + 46, BYTE_ORDER);
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

    public long holdAmount()
    {
        return buffer.getLong(offset + 50, BYTE_ORDER);
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

    public long acceptedAtMs()
    {
        return buffer.getLong(offset + 58, BYTE_ORDER);
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

    public long eventSeq()
    {
        return buffer.getLong(offset + 66, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final OrderAckDecoder decoder = new OrderAckDecoder();
        decoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        return decoder.appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final int originalLimit = limit();
        limit(offset + actingBlockLength);
        builder.append("[OrderAck](sbeTemplateId=");
        builder.append(TEMPLATE_ID);
        builder.append("|sbeSchemaId=");
        builder.append(SCHEMA_ID);
        builder.append("|sbeSchemaVersion=");
        if (parentMessage.actingVersion != SCHEMA_VERSION)
        {
            builder.append(parentMessage.actingVersion);
            builder.append('/');
        }
        builder.append(SCHEMA_VERSION);
        builder.append("|sbeBlockLength=");
        if (actingBlockLength != BLOCK_LENGTH)
        {
            builder.append(actingBlockLength);
            builder.append('/');
        }
        builder.append(BLOCK_LENGTH);
        builder.append("):");
        builder.append("correlationId=");
        builder.append(this.correlationId());
        builder.append('|');
        builder.append("requestIdHigh=");
        builder.append(this.requestIdHigh());
        builder.append('|');
        builder.append("requestIdLow=");
        builder.append(this.requestIdLow());
        builder.append('|');
        builder.append("orderId=");
        builder.append(this.orderId());
        builder.append('|');
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("status=");
        builder.append(this.status());
        builder.append('|');
        builder.append("duplicate=");
        builder.append(this.duplicate());
        builder.append('|');
        builder.append("holdAssetId=");
        builder.append(this.holdAssetId());
        builder.append('|');
        builder.append("holdAmount=");
        builder.append(this.holdAmount());
        builder.append('|');
        builder.append("acceptedAtMs=");
        builder.append(this.acceptedAtMs());
        builder.append('|');
        builder.append("eventSeq=");
        builder.append(this.eventSeq());

        limit(originalLimit);

        return builder;
    }
    
    public OrderAckDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
