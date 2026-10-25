/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * BROADCAST orders: one copy of every order event to every gateway (backend scoping by userId at the gateway). eventSeq is dense, replicated, snapshotted (R03 IN-06). Fat variant per critic D15 (+timeInForce/stopPrice/createdAtMs for the RC4 projector).
 */
@SuppressWarnings("all")
public final class OrderEventEncoder
{
    public static final int BLOCK_LENGTH = 166;
    public static final int TEMPLATE_ID = 102;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderEventEncoder parentMessage = this;
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

    public OrderEventEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public OrderEventEncoder wrapAndApplyHeader(
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

    public static int eventSeqId()
    {
        return 1;
    }

    public static int eventSeqSinceVersion()
    {
        return 0;
    }

    public static int eventSeqEncodingOffset()
    {
        return 0;
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

    public OrderEventEncoder eventSeq(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int logPositionId()
    {
        return 2;
    }

    public static int logPositionSinceVersion()
    {
        return 0;
    }

    public static int logPositionEncodingOffset()
    {
        return 8;
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

    public OrderEventEncoder logPosition(final long value)
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

    public OrderEventEncoder orderId(final long value)
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

    public OrderEventEncoder userId(final long value)
    {
        buffer.putLong(offset + 24, value, BYTE_ORDER);
        return this;
    }


    public static int marketIdId()
    {
        return 5;
    }

    public static int marketIdSinceVersion()
    {
        return 0;
    }

    public static int marketIdEncodingOffset()
    {
        return 32;
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

    public OrderEventEncoder marketId(final int value)
    {
        buffer.putInt(offset + 32, value, BYTE_ORDER);
        return this;
    }


    public static int eventKindId()
    {
        return 6;
    }

    public static int eventKindSinceVersion()
    {
        return 0;
    }

    public static int eventKindEncodingOffset()
    {
        return 36;
    }

    public static int eventKindEncodingLength()
    {
        return 1;
    }

    public static String eventKindMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public OrderEventEncoder eventKind(final EventKind value)
    {
        buffer.putByte(offset + 36, (byte)value.value());
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
        return 37;
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

    public OrderEventEncoder status(final OrderStatus value)
    {
        buffer.putByte(offset + 37, (byte)value.value());
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
        return 38;
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

    public OrderEventEncoder reason(final CoreRejectReason value)
    {
        buffer.putByte(offset + 38, (byte)value.value());
        return this;
    }

    public static int sideId()
    {
        return 9;
    }

    public static int sideSinceVersion()
    {
        return 0;
    }

    public static int sideEncodingOffset()
    {
        return 39;
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

    public OrderEventEncoder side(final Side value)
    {
        buffer.putByte(offset + 39, (byte)value.value());
        return this;
    }

    public static int orderTypeId()
    {
        return 10;
    }

    public static int orderTypeSinceVersion()
    {
        return 0;
    }

    public static int orderTypeEncodingOffset()
    {
        return 40;
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

    public OrderEventEncoder orderType(final OrderType value)
    {
        buffer.putByte(offset + 40, (byte)value.value());
        return this;
    }

    public static int timeInForceId()
    {
        return 11;
    }

    public static int timeInForceSinceVersion()
    {
        return 0;
    }

    public static int timeInForceEncodingOffset()
    {
        return 41;
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

    public OrderEventEncoder timeInForce(final TimeInForce value)
    {
        buffer.putByte(offset + 41, (byte)value.value());
        return this;
    }

    public static int priceId()
    {
        return 12;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 42;
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

    public OrderEventEncoder price(final long value)
    {
        buffer.putLong(offset + 42, value, BYTE_ORDER);
        return this;
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
        return 50;
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

    public OrderEventEncoder stopPrice(final long value)
    {
        buffer.putLong(offset + 50, value, BYTE_ORDER);
        return this;
    }


    public static int quantityId()
    {
        return 14;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 58;
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

    public OrderEventEncoder quantity(final long value)
    {
        buffer.putLong(offset + 58, value, BYTE_ORDER);
        return this;
    }


    public static int filledQtyId()
    {
        return 15;
    }

    public static int filledQtySinceVersion()
    {
        return 0;
    }

    public static int filledQtyEncodingOffset()
    {
        return 66;
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

    public OrderEventEncoder filledQty(final long value)
    {
        buffer.putLong(offset + 66, value, BYTE_ORDER);
        return this;
    }


    public static int remainingQtyId()
    {
        return 16;
    }

    public static int remainingQtySinceVersion()
    {
        return 0;
    }

    public static int remainingQtyEncodingOffset()
    {
        return 74;
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

    public OrderEventEncoder remainingQty(final long value)
    {
        buffer.putLong(offset + 74, value, BYTE_ORDER);
        return this;
    }


    public static int lastPriceId()
    {
        return 17;
    }

    public static int lastPriceSinceVersion()
    {
        return 0;
    }

    public static int lastPriceEncodingOffset()
    {
        return 82;
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

    public OrderEventEncoder lastPrice(final long value)
    {
        buffer.putLong(offset + 82, value, BYTE_ORDER);
        return this;
    }


    public static int lastQtyId()
    {
        return 18;
    }

    public static int lastQtySinceVersion()
    {
        return 0;
    }

    public static int lastQtyEncodingOffset()
    {
        return 90;
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

    public OrderEventEncoder lastQty(final long value)
    {
        buffer.putLong(offset + 90, value, BYTE_ORDER);
        return this;
    }


    public static int tradeIdId()
    {
        return 19;
    }

    public static int tradeIdSinceVersion()
    {
        return 0;
    }

    public static int tradeIdEncodingOffset()
    {
        return 98;
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

    public OrderEventEncoder tradeId(final long value)
    {
        buffer.putLong(offset + 98, value, BYTE_ORDER);
        return this;
    }


    public static int meOrderIdId()
    {
        return 20;
    }

    public static int meOrderIdSinceVersion()
    {
        return 0;
    }

    public static int meOrderIdEncodingOffset()
    {
        return 106;
    }

    public static int meOrderIdEncodingLength()
    {
        return 8;
    }

    public static String meOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long meOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long meOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long meOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderEventEncoder meOrderId(final long value)
    {
        buffer.putLong(offset + 106, value, BYTE_ORDER);
        return this;
    }


    public static int holdAssetIdId()
    {
        return 21;
    }

    public static int holdAssetIdSinceVersion()
    {
        return 0;
    }

    public static int holdAssetIdEncodingOffset()
    {
        return 114;
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

    public OrderEventEncoder holdAssetId(final int value)
    {
        buffer.putInt(offset + 114, value, BYTE_ORDER);
        return this;
    }


    public static int holdAmountId()
    {
        return 22;
    }

    public static int holdAmountSinceVersion()
    {
        return 0;
    }

    public static int holdAmountEncodingOffset()
    {
        return 118;
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

    public OrderEventEncoder holdAmount(final long value)
    {
        buffer.putLong(offset + 118, value, BYTE_ORDER);
        return this;
    }


    public static int holdRemainingId()
    {
        return 23;
    }

    public static int holdRemainingSinceVersion()
    {
        return 0;
    }

    public static int holdRemainingEncodingOffset()
    {
        return 126;
    }

    public static int holdRemainingEncodingLength()
    {
        return 8;
    }

    public static String holdRemainingMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long holdRemainingNullValue()
    {
        return -9223372036854775808L;
    }

    public static long holdRemainingMinValue()
    {
        return -9223372036854775807L;
    }

    public static long holdRemainingMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderEventEncoder holdRemaining(final long value)
    {
        buffer.putLong(offset + 126, value, BYTE_ORDER);
        return this;
    }


    public static int clientOrderIdHighId()
    {
        return 24;
    }

    public static int clientOrderIdHighSinceVersion()
    {
        return 0;
    }

    public static int clientOrderIdHighEncodingOffset()
    {
        return 134;
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

    public OrderEventEncoder clientOrderIdHigh(final long value)
    {
        buffer.putLong(offset + 134, value, BYTE_ORDER);
        return this;
    }


    public static int clientOrderIdLowId()
    {
        return 25;
    }

    public static int clientOrderIdLowSinceVersion()
    {
        return 0;
    }

    public static int clientOrderIdLowEncodingOffset()
    {
        return 142;
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

    public OrderEventEncoder clientOrderIdLow(final long value)
    {
        buffer.putLong(offset + 142, value, BYTE_ORDER);
        return this;
    }


    public static int createdAtMsId()
    {
        return 26;
    }

    public static int createdAtMsSinceVersion()
    {
        return 0;
    }

    public static int createdAtMsEncodingOffset()
    {
        return 150;
    }

    public static int createdAtMsEncodingLength()
    {
        return 8;
    }

    public static String createdAtMsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long createdAtMsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long createdAtMsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long createdAtMsMaxValue()
    {
        return 9223372036854775807L;
    }

    public OrderEventEncoder createdAtMs(final long value)
    {
        buffer.putLong(offset + 150, value, BYTE_ORDER);
        return this;
    }


    public static int updatedAtMsId()
    {
        return 27;
    }

    public static int updatedAtMsSinceVersion()
    {
        return 0;
    }

    public static int updatedAtMsEncodingOffset()
    {
        return 158;
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

    public OrderEventEncoder updatedAtMs(final long value)
    {
        buffer.putLong(offset + 158, value, BYTE_ORDER);
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

        final OrderEventDecoder decoder = new OrderEventDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
