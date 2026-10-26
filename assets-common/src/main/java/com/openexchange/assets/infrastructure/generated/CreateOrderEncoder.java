/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Place an order in the core (replaces the OMS REST create). budget: MARKET BUY quote cap, 0 = core default from the last MarketTick; must be 0 otherwise. expiresAtMs: GTD cluster-time deadline.
 */
@SuppressWarnings("all")
public final class CreateOrderEncoder
{
    public static final int BLOCK_LENGTH = 115;
    public static final int TEMPLATE_ID = 50;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CreateOrderEncoder parentMessage = this;
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

    public CreateOrderEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public CreateOrderEncoder wrapAndApplyHeader(
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

    public CreateOrderEncoder correlationId(final long value)
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

    public CreateOrderEncoder userId(final long value)
    {
        buffer.putLong(offset + 8, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder clientOrderIdHigh(final long value)
    {
        buffer.putLong(offset + 16, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder clientOrderIdLow(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder requestIdHigh(final long value)
    {
        buffer.putLong(offset + 32, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder requestIdLow(final long value)
    {
        buffer.putLong(offset + 40, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder marketId(final int value)
    {
        buffer.putInt(offset + 48, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder side(final Side value)
    {
        buffer.putByte(offset + 52, (byte)value.value());
        return this;
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

    public CreateOrderEncoder orderType(final OrderType value)
    {
        buffer.putByte(offset + 53, (byte)value.value());
        return this;
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

    public CreateOrderEncoder timeInForce(final TimeInForce value)
    {
        buffer.putByte(offset + 54, (byte)value.value());
        return this;
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

    public CreateOrderEncoder price(final long value)
    {
        buffer.putLong(offset + 55, value, BYTE_ORDER);
        return this;
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

    public CreateOrderEncoder quantity(final long value)
    {
        buffer.putLong(offset + 63, value, BYTE_ORDER);
        return this;
    }


    public static int budgetId()
    {
        return 13;
    }

    public static int budgetSinceVersion()
    {
        return 0;
    }

    public static int budgetEncodingOffset()
    {
        return 71;
    }

    public static int budgetEncodingLength()
    {
        return 8;
    }

    public static String budgetMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long budgetNullValue()
    {
        return -9223372036854775808L;
    }

    public static long budgetMinValue()
    {
        return -9223372036854775807L;
    }

    public static long budgetMaxValue()
    {
        return 9223372036854775807L;
    }

    public CreateOrderEncoder budget(final long value)
    {
        buffer.putLong(offset + 71, value, BYTE_ORDER);
        return this;
    }


    public static int stopPriceId()
    {
        return 14;
    }

    public static int stopPriceSinceVersion()
    {
        return 0;
    }

    public static int stopPriceEncodingOffset()
    {
        return 79;
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

    public CreateOrderEncoder stopPrice(final long value)
    {
        buffer.putLong(offset + 79, value, BYTE_ORDER);
        return this;
    }


    public static int trailingDeltaId()
    {
        return 15;
    }

    public static int trailingDeltaSinceVersion()
    {
        return 0;
    }

    public static int trailingDeltaEncodingOffset()
    {
        return 87;
    }

    public static int trailingDeltaEncodingLength()
    {
        return 8;
    }

    public static String trailingDeltaMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long trailingDeltaNullValue()
    {
        return -9223372036854775808L;
    }

    public static long trailingDeltaMinValue()
    {
        return -9223372036854775807L;
    }

    public static long trailingDeltaMaxValue()
    {
        return 9223372036854775807L;
    }

    public CreateOrderEncoder trailingDelta(final long value)
    {
        buffer.putLong(offset + 87, value, BYTE_ORDER);
        return this;
    }


    public static int displayQtyId()
    {
        return 16;
    }

    public static int displayQtySinceVersion()
    {
        return 0;
    }

    public static int displayQtyEncodingOffset()
    {
        return 95;
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

    public CreateOrderEncoder displayQty(final long value)
    {
        buffer.putLong(offset + 95, value, BYTE_ORDER);
        return this;
    }


    public static int expiresAtMsId()
    {
        return 17;
    }

    public static int expiresAtMsSinceVersion()
    {
        return 0;
    }

    public static int expiresAtMsEncodingOffset()
    {
        return 103;
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

    public CreateOrderEncoder expiresAtMs(final long value)
    {
        buffer.putLong(offset + 103, value, BYTE_ORDER);
        return this;
    }


    public static int flagsId()
    {
        return 18;
    }

    public static int flagsSinceVersion()
    {
        return 0;
    }

    public static int flagsEncodingOffset()
    {
        return 111;
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

    private final OrderFlagsEncoder flags = new OrderFlagsEncoder();

    public OrderFlagsEncoder flags()
    {
        flags.wrap(buffer, offset + 111);
        return flags;
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

        final CreateOrderDecoder decoder = new CreateOrderDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
