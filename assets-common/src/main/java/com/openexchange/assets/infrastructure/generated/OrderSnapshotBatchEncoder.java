/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * ORIGIN reply chunk to RequestOrderSnapshot (cap 56), ordered by orderId.
 */
@SuppressWarnings("all")
public final class OrderSnapshotBatchEncoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 106;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderSnapshotBatchEncoder parentMessage = this;
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

    public OrderSnapshotBatchEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public OrderSnapshotBatchEncoder wrapAndApplyHeader(
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

    private final OrdersEncoder orders = new OrdersEncoder(this);

    public static long ordersId()
    {
        return 20;
    }

    public OrdersEncoder ordersCount(final int count)
    {
        orders.wrap(buffer, count);
        return orders;
    }

    public static final class OrdersEncoder
    {
        public static final int HEADER_SIZE = 4;
        private final OrderSnapshotBatchEncoder parentMessage;
        private MutableDirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int initialLimit;

        OrdersEncoder(final OrderSnapshotBatchEncoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final MutableDirectBuffer buffer, final int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new IllegalArgumentException("count outside allowed range: count=" + count);
            }

            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            this.count = count;
            final int limit = parentMessage.limit();
            initialLimit = limit;
            parentMessage.limit(limit + HEADER_SIZE);
            buffer.putShort(limit + 0, (short)160, BYTE_ORDER);
            buffer.putShort(limit + 2, (short)count, BYTE_ORDER);
        }

        public OrdersEncoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + sbeBlockLength());
            ++index;

            return this;
        }

        public int resetCountToIndex()
        {
            count = index;
            buffer.putShort(initialLimit + 2, (short)count, BYTE_ORDER);

            return count;
        }

        public static int countMinValue()
        {
            return 0;
        }

        public static int countMaxValue()
        {
            return 65534;
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 160;
        }

        public static int orderIdId()
        {
            return 1;
        }

        public static int orderIdSinceVersion()
        {
            return 0;
        }

        public static int orderIdEncodingOffset()
        {
            return 0;
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

        public OrdersEncoder orderId(final long value)
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

        public OrdersEncoder userId(final long value)
        {
            buffer.putLong(offset + 8, value, BYTE_ORDER);
            return this;
        }


        public static int marketIdId()
        {
            return 3;
        }

        public static int marketIdSinceVersion()
        {
            return 0;
        }

        public static int marketIdEncodingOffset()
        {
            return 16;
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

        public OrdersEncoder marketId(final int value)
        {
            buffer.putInt(offset + 16, value, BYTE_ORDER);
            return this;
        }


        public static int statusId()
        {
            return 4;
        }

        public static int statusSinceVersion()
        {
            return 0;
        }

        public static int statusEncodingOffset()
        {
            return 20;
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

        public OrdersEncoder status(final OrderStatus value)
        {
            buffer.putByte(offset + 20, (byte)value.value());
            return this;
        }

        public static int sideId()
        {
            return 5;
        }

        public static int sideSinceVersion()
        {
            return 0;
        }

        public static int sideEncodingOffset()
        {
            return 21;
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

        public OrdersEncoder side(final Side value)
        {
            buffer.putByte(offset + 21, (byte)value.value());
            return this;
        }

        public static int orderTypeId()
        {
            return 6;
        }

        public static int orderTypeSinceVersion()
        {
            return 0;
        }

        public static int orderTypeEncodingOffset()
        {
            return 22;
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

        public OrdersEncoder orderType(final OrderType value)
        {
            buffer.putByte(offset + 22, (byte)value.value());
            return this;
        }

        public static int timeInForceId()
        {
            return 7;
        }

        public static int timeInForceSinceVersion()
        {
            return 0;
        }

        public static int timeInForceEncodingOffset()
        {
            return 23;
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

        public OrdersEncoder timeInForce(final TimeInForce value)
        {
            buffer.putByte(offset + 23, (byte)value.value());
            return this;
        }

        public static int priceId()
        {
            return 8;
        }

        public static int priceSinceVersion()
        {
            return 0;
        }

        public static int priceEncodingOffset()
        {
            return 24;
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

        public OrdersEncoder price(final long value)
        {
            buffer.putLong(offset + 24, value, BYTE_ORDER);
            return this;
        }


        public static int quantityId()
        {
            return 9;
        }

        public static int quantitySinceVersion()
        {
            return 0;
        }

        public static int quantityEncodingOffset()
        {
            return 32;
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

        public OrdersEncoder quantity(final long value)
        {
            buffer.putLong(offset + 32, value, BYTE_ORDER);
            return this;
        }


        public static int filledQtyId()
        {
            return 10;
        }

        public static int filledQtySinceVersion()
        {
            return 0;
        }

        public static int filledQtyEncodingOffset()
        {
            return 40;
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

        public OrdersEncoder filledQty(final long value)
        {
            buffer.putLong(offset + 40, value, BYTE_ORDER);
            return this;
        }


        public static int remainingQtyId()
        {
            return 11;
        }

        public static int remainingQtySinceVersion()
        {
            return 0;
        }

        public static int remainingQtyEncodingOffset()
        {
            return 48;
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

        public OrdersEncoder remainingQty(final long value)
        {
            buffer.putLong(offset + 48, value, BYTE_ORDER);
            return this;
        }


        public static int stopPriceId()
        {
            return 12;
        }

        public static int stopPriceSinceVersion()
        {
            return 0;
        }

        public static int stopPriceEncodingOffset()
        {
            return 56;
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

        public OrdersEncoder stopPrice(final long value)
        {
            buffer.putLong(offset + 56, value, BYTE_ORDER);
            return this;
        }


        public static int displayQtyId()
        {
            return 13;
        }

        public static int displayQtySinceVersion()
        {
            return 0;
        }

        public static int displayQtyEncodingOffset()
        {
            return 64;
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

        public OrdersEncoder displayQty(final long value)
        {
            buffer.putLong(offset + 64, value, BYTE_ORDER);
            return this;
        }


        public static int expiresAtMsId()
        {
            return 14;
        }

        public static int expiresAtMsSinceVersion()
        {
            return 0;
        }

        public static int expiresAtMsEncodingOffset()
        {
            return 72;
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

        public OrdersEncoder expiresAtMs(final long value)
        {
            buffer.putLong(offset + 72, value, BYTE_ORDER);
            return this;
        }


        public static int holdAssetIdId()
        {
            return 15;
        }

        public static int holdAssetIdSinceVersion()
        {
            return 0;
        }

        public static int holdAssetIdEncodingOffset()
        {
            return 80;
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

        public OrdersEncoder holdAssetId(final int value)
        {
            buffer.putInt(offset + 80, value, BYTE_ORDER);
            return this;
        }


        public static int holdAmountId()
        {
            return 16;
        }

        public static int holdAmountSinceVersion()
        {
            return 0;
        }

        public static int holdAmountEncodingOffset()
        {
            return 84;
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

        public OrdersEncoder holdAmount(final long value)
        {
            buffer.putLong(offset + 84, value, BYTE_ORDER);
            return this;
        }


        public static int clientOrderIdHighId()
        {
            return 17;
        }

        public static int clientOrderIdHighSinceVersion()
        {
            return 0;
        }

        public static int clientOrderIdHighEncodingOffset()
        {
            return 92;
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

        public OrdersEncoder clientOrderIdHigh(final long value)
        {
            buffer.putLong(offset + 92, value, BYTE_ORDER);
            return this;
        }


        public static int clientOrderIdLowId()
        {
            return 18;
        }

        public static int clientOrderIdLowSinceVersion()
        {
            return 0;
        }

        public static int clientOrderIdLowEncodingOffset()
        {
            return 100;
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

        public OrdersEncoder clientOrderIdLow(final long value)
        {
            buffer.putLong(offset + 100, value, BYTE_ORDER);
            return this;
        }


        public static int meOrderIdId()
        {
            return 19;
        }

        public static int meOrderIdSinceVersion()
        {
            return 0;
        }

        public static int meOrderIdEncodingOffset()
        {
            return 108;
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

        public OrdersEncoder meOrderId(final long value)
        {
            buffer.putLong(offset + 108, value, BYTE_ORDER);
            return this;
        }


        public static int trailingArmId()
        {
            return 20;
        }

        public static int trailingArmSinceVersion()
        {
            return 0;
        }

        public static int trailingArmEncodingOffset()
        {
            return 116;
        }

        public static int trailingArmEncodingLength()
        {
            return 8;
        }

        public static String trailingArmMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long trailingArmNullValue()
        {
            return -9223372036854775808L;
        }

        public static long trailingArmMinValue()
        {
            return -9223372036854775807L;
        }

        public static long trailingArmMaxValue()
        {
            return 9223372036854775807L;
        }

        public OrdersEncoder trailingArm(final long value)
        {
            buffer.putLong(offset + 116, value, BYTE_ORDER);
            return this;
        }


        public static int hiddenQtyId()
        {
            return 21;
        }

        public static int hiddenQtySinceVersion()
        {
            return 0;
        }

        public static int hiddenQtyEncodingOffset()
        {
            return 124;
        }

        public static int hiddenQtyEncodingLength()
        {
            return 8;
        }

        public static String hiddenQtyMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long hiddenQtyNullValue()
        {
            return -9223372036854775808L;
        }

        public static long hiddenQtyMinValue()
        {
            return -9223372036854775807L;
        }

        public static long hiddenQtyMaxValue()
        {
            return 9223372036854775807L;
        }

        public OrdersEncoder hiddenQty(final long value)
        {
            buffer.putLong(offset + 124, value, BYTE_ORDER);
            return this;
        }


        public static int sliceSeqId()
        {
            return 22;
        }

        public static int sliceSeqSinceVersion()
        {
            return 0;
        }

        public static int sliceSeqEncodingOffset()
        {
            return 132;
        }

        public static int sliceSeqEncodingLength()
        {
            return 4;
        }

        public static String sliceSeqMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int sliceSeqNullValue()
        {
            return -2147483648;
        }

        public static int sliceSeqMinValue()
        {
            return -2147483647;
        }

        public static int sliceSeqMaxValue()
        {
            return 2147483647;
        }

        public OrdersEncoder sliceSeq(final int value)
        {
            buffer.putInt(offset + 132, value, BYTE_ORDER);
            return this;
        }


        public static int budgetId()
        {
            return 23;
        }

        public static int budgetSinceVersion()
        {
            return 0;
        }

        public static int budgetEncodingOffset()
        {
            return 136;
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

        public OrdersEncoder budget(final long value)
        {
            buffer.putLong(offset + 136, value, BYTE_ORDER);
            return this;
        }


        public static int createdAtMsId()
        {
            return 24;
        }

        public static int createdAtMsSinceVersion()
        {
            return 0;
        }

        public static int createdAtMsEncodingOffset()
        {
            return 144;
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

        public OrdersEncoder createdAtMs(final long value)
        {
            buffer.putLong(offset + 144, value, BYTE_ORDER);
            return this;
        }


        public static int updatedAtMsId()
        {
            return 25;
        }

        public static int updatedAtMsSinceVersion()
        {
            return 0;
        }

        public static int updatedAtMsEncodingOffset()
        {
            return 152;
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

        public OrdersEncoder updatedAtMs(final long value)
        {
            buffer.putLong(offset + 152, value, BYTE_ORDER);
            return this;
        }

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

        final OrderSnapshotBatchDecoder decoder = new OrderSnapshotBatchDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
