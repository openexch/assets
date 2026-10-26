/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * ORIGIN reply chunk to RequestOrderSnapshot (cap 56), ordered by orderId.
 */
@SuppressWarnings("all")
public final class OrderSnapshotBatchDecoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 106;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderSnapshotBatchDecoder parentMessage = this;
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

    public OrderSnapshotBatchDecoder wrap(
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

    public OrderSnapshotBatchDecoder wrapAndApplyHeader(
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

    public OrderSnapshotBatchDecoder sbeRewind()
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

    private final OrdersDecoder orders = new OrdersDecoder(this);

    public static long ordersDecoderId()
    {
        return 20;
    }

    public static int ordersDecoderSinceVersion()
    {
        return 0;
    }

    public OrdersDecoder orders()
    {
        orders.wrap(buffer);
        return orders;
    }

    public static final class OrdersDecoder
        implements Iterable<OrdersDecoder>, java.util.Iterator<OrdersDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final OrderSnapshotBatchDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        OrdersDecoder(final OrderSnapshotBatchDecoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final DirectBuffer buffer)
        {
            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            final int limit = parentMessage.limit();
            parentMessage.limit(limit + HEADER_SIZE);
            blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
            count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
        }

        public OrdersDecoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
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

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int actingVersion()
        {
            return parentMessage.actingVersion;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<OrdersDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return index < count;
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

        public long orderId()
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

        public int marketId()
        {
            return buffer.getInt(offset + 16, BYTE_ORDER);
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

        public short statusRaw()
        {
            return ((short)(buffer.getByte(offset + 20) & 0xFF));
        }

        public OrderStatus status()
        {
            return OrderStatus.get(((short)(buffer.getByte(offset + 20) & 0xFF)));
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

        public short sideRaw()
        {
            return ((short)(buffer.getByte(offset + 21) & 0xFF));
        }

        public Side side()
        {
            return Side.get(((short)(buffer.getByte(offset + 21) & 0xFF)));
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

        public short orderTypeRaw()
        {
            return ((short)(buffer.getByte(offset + 22) & 0xFF));
        }

        public OrderType orderType()
        {
            return OrderType.get(((short)(buffer.getByte(offset + 22) & 0xFF)));
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

        public short timeInForceRaw()
        {
            return ((short)(buffer.getByte(offset + 23) & 0xFF));
        }

        public TimeInForce timeInForce()
        {
            return TimeInForce.get(((short)(buffer.getByte(offset + 23) & 0xFF)));
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

        public long price()
        {
            return buffer.getLong(offset + 24, BYTE_ORDER);
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

        public long quantity()
        {
            return buffer.getLong(offset + 32, BYTE_ORDER);
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

        public long filledQty()
        {
            return buffer.getLong(offset + 40, BYTE_ORDER);
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

        public long remainingQty()
        {
            return buffer.getLong(offset + 48, BYTE_ORDER);
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

        public long stopPrice()
        {
            return buffer.getLong(offset + 56, BYTE_ORDER);
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

        public long displayQty()
        {
            return buffer.getLong(offset + 64, BYTE_ORDER);
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

        public long expiresAtMs()
        {
            return buffer.getLong(offset + 72, BYTE_ORDER);
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

        public int holdAssetId()
        {
            return buffer.getInt(offset + 80, BYTE_ORDER);
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

        public long holdAmount()
        {
            return buffer.getLong(offset + 84, BYTE_ORDER);
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

        public long clientOrderIdHigh()
        {
            return buffer.getLong(offset + 92, BYTE_ORDER);
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

        public long clientOrderIdLow()
        {
            return buffer.getLong(offset + 100, BYTE_ORDER);
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

        public long meOrderId()
        {
            return buffer.getLong(offset + 108, BYTE_ORDER);
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

        public long trailingArm()
        {
            return buffer.getLong(offset + 116, BYTE_ORDER);
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

        public long hiddenQty()
        {
            return buffer.getLong(offset + 124, BYTE_ORDER);
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

        public int sliceSeq()
        {
            return buffer.getInt(offset + 132, BYTE_ORDER);
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

        public long budget()
        {
            return buffer.getLong(offset + 136, BYTE_ORDER);
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

        public long createdAtMs()
        {
            return buffer.getLong(offset + 144, BYTE_ORDER);
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

        public long updatedAtMs()
        {
            return buffer.getLong(offset + 152, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
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
            builder.append("filledQty=");
            builder.append(this.filledQty());
            builder.append('|');
            builder.append("remainingQty=");
            builder.append(this.remainingQty());
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
            builder.append("holdAssetId=");
            builder.append(this.holdAssetId());
            builder.append('|');
            builder.append("holdAmount=");
            builder.append(this.holdAmount());
            builder.append('|');
            builder.append("clientOrderIdHigh=");
            builder.append(this.clientOrderIdHigh());
            builder.append('|');
            builder.append("clientOrderIdLow=");
            builder.append(this.clientOrderIdLow());
            builder.append('|');
            builder.append("meOrderId=");
            builder.append(this.meOrderId());
            builder.append('|');
            builder.append("trailingArm=");
            builder.append(this.trailingArm());
            builder.append('|');
            builder.append("hiddenQty=");
            builder.append(this.hiddenQty());
            builder.append('|');
            builder.append("sliceSeq=");
            builder.append(this.sliceSeq());
            builder.append('|');
            builder.append("budget=");
            builder.append(this.budget());
            builder.append('|');
            builder.append("createdAtMs=");
            builder.append(this.createdAtMs());
            builder.append('|');
            builder.append("updatedAtMs=");
            builder.append(this.updatedAtMs());
            builder.append(')');

            return builder;
        }
        
        public OrdersDecoder sbeSkip()
        {

            return this;
        }
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final OrderSnapshotBatchDecoder decoder = new OrderSnapshotBatchDecoder();
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
        builder.append("[OrderSnapshotBatch](sbeTemplateId=");
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
        builder.append("orders=[");
        final int ordersOriginalOffset = orders.offset;
        final int ordersOriginalIndex = orders.index;
        final OrdersDecoder orders = this.orders();
        if (orders.count() > 0)
        {
            while (orders.hasNext())
            {
                orders.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        orders.offset = ordersOriginalOffset;
        orders.index = ordersOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public OrderSnapshotBatchDecoder sbeSkip()
    {
        sbeRewind();
        OrdersDecoder orders = this.orders();
        if (orders.count() > 0)
        {
            while (orders.hasNext())
            {
                orders.next();
                orders.sbeSkip();
            }
        }

        return this;
    }
}
