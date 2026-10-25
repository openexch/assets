/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * RC0 spike egress: one order lifecycle event
 */
@SuppressWarnings("all")
public final class ProtoOrderEventDecoder
{
    public static final int BLOCK_LENGTH = 86;
    public static final int TEMPLATE_ID = 43;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 5;
    public static final String SEMANTIC_VERSION = "0.5";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final ProtoOrderEventDecoder parentMessage = this;
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

    public ProtoOrderEventDecoder wrap(
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

    public ProtoOrderEventDecoder wrapAndApplyHeader(
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

    public ProtoOrderEventDecoder sbeRewind()
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

    public long orderId()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
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

    public long userId()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
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

    public int marketId()
    {
        return buffer.getInt(offset + 24, BYTE_ORDER);
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

    public short statusRaw()
    {
        return ((short)(buffer.getByte(offset + 28) & 0xFF));
    }

    public ProtoOrderStatus status()
    {
        return ProtoOrderStatus.get(((short)(buffer.getByte(offset + 28) & 0xFF)));
    }


    public static int reasonId()
    {
        return 6;
    }

    public static int reasonSinceVersion()
    {
        return 0;
    }

    public static int reasonEncodingOffset()
    {
        return 29;
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

    public short reasonRaw()
    {
        return ((short)(buffer.getByte(offset + 29) & 0xFF));
    }

    public ProtoRejectReason reason()
    {
        return ProtoRejectReason.get(((short)(buffer.getByte(offset + 29) & 0xFF)));
    }


    public static int filledQtyId()
    {
        return 7;
    }

    public static int filledQtySinceVersion()
    {
        return 0;
    }

    public static int filledQtyEncodingOffset()
    {
        return 30;
    }

    public static int filledQtyEncodingLength()
    {
        return 8;
    }

    public static String filledQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long filledQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long filledQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long filledQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long filledQty()
    {
        return buffer.getLong(offset + 30, BYTE_ORDER);
    }


    public static int remainingQtyId()
    {
        return 8;
    }

    public static int remainingQtySinceVersion()
    {
        return 0;
    }

    public static int remainingQtyEncodingOffset()
    {
        return 38;
    }

    public static int remainingQtyEncodingLength()
    {
        return 8;
    }

    public static String remainingQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long remainingQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long remainingQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long remainingQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long remainingQty()
    {
        return buffer.getLong(offset + 38, BYTE_ORDER);
    }


    public static int lastPriceId()
    {
        return 9;
    }

    public static int lastPriceSinceVersion()
    {
        return 0;
    }

    public static int lastPriceEncodingOffset()
    {
        return 46;
    }

    public static int lastPriceEncodingLength()
    {
        return 8;
    }

    public static String lastPriceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastPriceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastPriceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastPriceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastPrice()
    {
        return buffer.getLong(offset + 46, BYTE_ORDER);
    }


    public static int lastQtyId()
    {
        return 10;
    }

    public static int lastQtySinceVersion()
    {
        return 0;
    }

    public static int lastQtyEncodingOffset()
    {
        return 54;
    }

    public static int lastQtyEncodingLength()
    {
        return 8;
    }

    public static String lastQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long lastQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long lastQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long lastQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long lastQty()
    {
        return buffer.getLong(offset + 54, BYTE_ORDER);
    }


    public static int holdAmountId()
    {
        return 11;
    }

    public static int holdAmountSinceVersion()
    {
        return 0;
    }

    public static int holdAmountEncodingOffset()
    {
        return 62;
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
        return buffer.getLong(offset + 62, BYTE_ORDER);
    }


    public static int updatedAtMsId()
    {
        return 12;
    }

    public static int updatedAtMsSinceVersion()
    {
        return 0;
    }

    public static int updatedAtMsEncodingOffset()
    {
        return 70;
    }

    public static int updatedAtMsEncodingLength()
    {
        return 8;
    }

    public static String updatedAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long updatedAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long updatedAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long updatedAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long updatedAtMs()
    {
        return buffer.getLong(offset + 70, BYTE_ORDER);
    }


    public static int eventSeqId()
    {
        return 13;
    }

    public static int eventSeqSinceVersion()
    {
        return 0;
    }

    public static int eventSeqEncodingOffset()
    {
        return 78;
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
        return buffer.getLong(offset + 78, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final ProtoOrderEventDecoder decoder = new ProtoOrderEventDecoder();
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
        builder.append("[ProtoOrderEvent](sbeTemplateId=");
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
        builder.append("reason=");
        builder.append(this.reason());
        builder.append('|');
        builder.append("filledQty=");
        builder.append(this.filledQty());
        builder.append('|');
        builder.append("remainingQty=");
        builder.append(this.remainingQty());
        builder.append('|');
        builder.append("lastPrice=");
        builder.append(this.lastPrice());
        builder.append('|');
        builder.append("lastQty=");
        builder.append(this.lastQty());
        builder.append('|');
        builder.append("holdAmount=");
        builder.append(this.holdAmount());
        builder.append('|');
        builder.append("updatedAtMs=");
        builder.append(this.updatedAtMs());
        builder.append('|');
        builder.append("eventSeq=");
        builder.append(this.eventSeq());

        limit(originalLimit);

        return builder;
    }
    
    public ProtoOrderEventDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
