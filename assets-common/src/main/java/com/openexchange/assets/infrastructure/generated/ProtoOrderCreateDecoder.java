/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * RC0 spike ingress: place an order in the core (risk + hold + record)
 */
@SuppressWarnings("all")
public final class ProtoOrderCreateDecoder
{
    public static final int BLOCK_LENGTH = 99;
    public static final int TEMPLATE_ID = 40;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 5;
    public static final String SEMANTIC_VERSION = "0.5";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final ProtoOrderCreateDecoder parentMessage = this;
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

    public ProtoOrderCreateDecoder wrap(
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

    public ProtoOrderCreateDecoder wrapAndApplyHeader(
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

    public ProtoOrderCreateDecoder sbeRewind()
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

    public long userId()
    {
        return buffer.getLong(offset + 8, BYTE_ORDER);
    }


    public static int clientOrderIdHighId()
    {
        return 3;
    }

    public static int clientOrderIdHighSinceVersion()
    {
        return 0;
    }

    public static int clientOrderIdHighEncodingOffset()
    {
        return 16;
    }

    public static int clientOrderIdHighEncodingLength()
    {
        return 8;
    }

    public static String clientOrderIdHighMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long clientOrderIdHighNullValue()
    {
        return -9223372036854775808L;
    }

    public static long clientOrderIdHighMinValue()
    {
        return -9223372036854775807L;
    }

    public static long clientOrderIdHighMaxValue()
    {
        return 9223372036854775807L;
    }

    public long clientOrderIdHigh()
    {
        return buffer.getLong(offset + 16, BYTE_ORDER);
    }


    public static int clientOrderIdLowId()
    {
        return 4;
    }

    public static int clientOrderIdLowSinceVersion()
    {
        return 0;
    }

    public static int clientOrderIdLowEncodingOffset()
    {
        return 24;
    }

    public static int clientOrderIdLowEncodingLength()
    {
        return 8;
    }

    public static String clientOrderIdLowMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long clientOrderIdLowNullValue()
    {
        return -9223372036854775808L;
    }

    public static long clientOrderIdLowMinValue()
    {
        return -9223372036854775807L;
    }

    public static long clientOrderIdLowMaxValue()
    {
        return 9223372036854775807L;
    }

    public long clientOrderIdLow()
    {
        return buffer.getLong(offset + 24, BYTE_ORDER);
    }


    public static int requestIdHighId()
    {
        return 5;
    }

    public static int requestIdHighSinceVersion()
    {
        return 0;
    }

    public static int requestIdHighEncodingOffset()
    {
        return 32;
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
        return buffer.getLong(offset + 32, BYTE_ORDER);
    }


    public static int requestIdLowId()
    {
        return 6;
    }

    public static int requestIdLowSinceVersion()
    {
        return 0;
    }

    public static int requestIdLowEncodingOffset()
    {
        return 40;
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
        return buffer.getLong(offset + 40, BYTE_ORDER);
    }


    public static int marketIdId()
    {
        return 7;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 48;
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
        return buffer.getInt(offset + 48, BYTE_ORDER);
    }


    public static int sideId()
    {
        return 8;
    }

    public static int sideSinceVersion()
    {
        return 0;
    }

    public static int sideEncodingOffset()
    {
        return 52;
    }

    public static int sideEncodingLength()
    {
        return 1;
    }

    public static String sideMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short sideRaw()
    {
        return ((short)(buffer.getByte(offset + 52) & 0xFF));
    }

    public ProtoSide side()
    {
        return ProtoSide.get(((short)(buffer.getByte(offset + 52) & 0xFF)));
    }


    public static int orderTypeId()
    {
        return 9;
    }

    public static int orderTypeSinceVersion()
    {
        return 0;
    }

    public static int orderTypeEncodingOffset()
    {
        return 53;
    }

    public static int orderTypeEncodingLength()
    {
        return 1;
    }

    public static String orderTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short orderTypeRaw()
    {
        return ((short)(buffer.getByte(offset + 53) & 0xFF));
    }

    public ProtoOrderType orderType()
    {
        return ProtoOrderType.get(((short)(buffer.getByte(offset + 53) & 0xFF)));
    }


    public static int timeInForceId()
    {
        return 10;
    }

    public static int timeInForceSinceVersion()
    {
        return 0;
    }

    public static int timeInForceEncodingOffset()
    {
        return 54;
    }

    public static int timeInForceEncodingLength()
    {
        return 1;
    }

    public static String timeInForceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short timeInForceRaw()
    {
        return ((short)(buffer.getByte(offset + 54) & 0xFF));
    }

    public ProtoTimeInForce timeInForce()
    {
        return ProtoTimeInForce.get(((short)(buffer.getByte(offset + 54) & 0xFF)));
    }


    public static int priceId()
    {
        return 11;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 55;
    }

    public static int priceEncodingLength()
    {
        return 8;
    }

    public static String priceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long priceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long priceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long priceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long price()
    {
        return buffer.getLong(offset + 55, BYTE_ORDER);
    }


    public static int quantityId()
    {
        return 12;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 63;
    }

    public static int quantityEncodingLength()
    {
        return 8;
    }

    public static String quantityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long quantityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long quantityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long quantityMaxValue()
    {
        return 9223372036854775807L;
    }

    public long quantity()
    {
        return buffer.getLong(offset + 63, BYTE_ORDER);
    }


    public static int stopPriceId()
    {
        return 13;
    }

    public static int stopPriceSinceVersion()
    {
        return 0;
    }

    public static int stopPriceEncodingOffset()
    {
        return 71;
    }

    public static int stopPriceEncodingLength()
    {
        return 8;
    }

    public static String stopPriceMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long stopPriceNullValue()
    {
        return -9223372036854775808L;
    }

    public static long stopPriceMinValue()
    {
        return -9223372036854775807L;
    }

    public static long stopPriceMaxValue()
    {
        return 9223372036854775807L;
    }

    public long stopPrice()
    {
        return buffer.getLong(offset + 71, BYTE_ORDER);
    }


    public static int displayQtyId()
    {
        return 14;
    }

    public static int displayQtySinceVersion()
    {
        return 0;
    }

    public static int displayQtyEncodingOffset()
    {
        return 79;
    }

    public static int displayQtyEncodingLength()
    {
        return 8;
    }

    public static String displayQtyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long displayQtyNullValue()
    {
        return -9223372036854775808L;
    }

    public static long displayQtyMinValue()
    {
        return -9223372036854775807L;
    }

    public static long displayQtyMaxValue()
    {
        return 9223372036854775807L;
    }

    public long displayQty()
    {
        return buffer.getLong(offset + 79, BYTE_ORDER);
    }


    public static int expiresAtMsId()
    {
        return 15;
    }

    public static int expiresAtMsSinceVersion()
    {
        return 0;
    }

    public static int expiresAtMsEncodingOffset()
    {
        return 87;
    }

    public static int expiresAtMsEncodingLength()
    {
        return 8;
    }

    public static String expiresAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long expiresAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long expiresAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long expiresAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long expiresAtMs()
    {
        return buffer.getLong(offset + 87, BYTE_ORDER);
    }


    public static int flagsId()
    {
        return 16;
    }

    public static int flagsSinceVersion()
    {
        return 0;
    }

    public static int flagsEncodingOffset()
    {
        return 95;
    }

    public static int flagsEncodingLength()
    {
        return 4;
    }

    public static String flagsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long flagsNullValue()
    {
        return 4294967295L;
    }

    public static long flagsMinValue()
    {
        return 0L;
    }

    public static long flagsMaxValue()
    {
        return 4294967294L;
    }

    public long flags()
    {
        return (buffer.getInt(offset + 95, BYTE_ORDER) & 0xFFFF_FFFFL);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final ProtoOrderCreateDecoder decoder = new ProtoOrderCreateDecoder();
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
        builder.append("[ProtoOrderCreate](sbeTemplateId=");
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
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("clientOrderIdHigh=");
        builder.append(this.clientOrderIdHigh());
        builder.append('|');
        builder.append("clientOrderIdLow=");
        builder.append(this.clientOrderIdLow());
        builder.append('|');
        builder.append("requestIdHigh=");
        builder.append(this.requestIdHigh());
        builder.append('|');
        builder.append("requestIdLow=");
        builder.append(this.requestIdLow());
        builder.append('|');
        builder.append("marketId=");
        builder.append(this.marketId());
        builder.append('|');
        builder.append("side=");
        builder.append(this.side());
        builder.append('|');
        builder.append("orderType=");
        builder.append(this.orderType());
        builder.append('|');
        builder.append("timeInForce=");
        builder.append(this.timeInForce());
        builder.append('|');
        builder.append("price=");
        builder.append(this.price());
        builder.append('|');
        builder.append("quantity=");
        builder.append(this.quantity());
        builder.append('|');
        builder.append("stopPrice=");
        builder.append(this.stopPrice());
        builder.append('|');
        builder.append("displayQty=");
        builder.append(this.displayQty());
        builder.append('|');
        builder.append("expiresAtMs=");
        builder.append(this.expiresAtMs());
        builder.append('|');
        builder.append("flags=");
        builder.append(this.flags());

        limit(originalLimit);

        return builder;
    }
    
    public ProtoOrderCreateDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
