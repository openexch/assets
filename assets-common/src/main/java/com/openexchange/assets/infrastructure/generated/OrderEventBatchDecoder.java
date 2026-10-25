/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * A run of OrderEvent entries formed at drain (cap 48 keeps the frame under the 8 KiB scratch).
 */
@SuppressWarnings("all")
public final class OrderEventBatchDecoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 103;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final OrderEventBatchDecoder parentMessage = this;
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

    public OrderEventBatchDecoder wrap(
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

    public OrderEventBatchDecoder wrapAndApplyHeader(
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

    public OrderEventBatchDecoder sbeRewind()
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

    private final EventsDecoder events = new EventsDecoder(this);

    public static long eventsDecoderId()
    {
        return 20;
    }

    public static int eventsDecoderSinceVersion()
    {
        return 0;
    }

    public EventsDecoder events()
    {
        events.wrap(buffer);
        return events;
    }

    public static final class EventsDecoder
        implements Iterable<EventsDecoder>, java.util.Iterator<EventsDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final OrderEventBatchDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        EventsDecoder(final OrderEventBatchDecoder parentMessage)
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

        public EventsDecoder next()
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
            return 166;
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

        public java.util.Iterator<EventsDecoder> iterator()
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

        public long eventSeq()
        {
            return buffer.getLong(offset + 0, BYTE_ORDER);
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

        public long logPosition()
        {
            return buffer.getLong(offset + 8, BYTE_ORDER);
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

        public long orderId()
        {
            return buffer.getLong(offset + 16, BYTE_ORDER);
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

        public long userId()
        {
            return buffer.getLong(offset + 24, BYTE_ORDER);
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

        public int marketId()
        {
            return buffer.getInt(offset + 32, BYTE_ORDER);
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

        public short eventKindRaw()
        {
            return ((short)(buffer.getByte(offset + 36) & 0xFF));
        }

        public EventKind eventKind()
        {
            return EventKind.get(((short)(buffer.getByte(offset + 36) & 0xFF)));
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

        public short statusRaw()
        {
            return ((short)(buffer.getByte(offset + 37) & 0xFF));
        }

        public OrderStatus status()
        {
            return OrderStatus.get(((short)(buffer.getByte(offset + 37) & 0xFF)));
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

        public short reasonRaw()
        {
            return ((short)(buffer.getByte(offset + 38) & 0xFF));
        }

        public CoreRejectReason reason()
        {
            return CoreRejectReason.get(((short)(buffer.getByte(offset + 38) & 0xFF)));
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

        public short sideRaw()
        {
            return ((short)(buffer.getByte(offset + 39) & 0xFF));
        }

        public Side side()
        {
            return Side.get(((short)(buffer.getByte(offset + 39) & 0xFF)));
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

        public short orderTypeRaw()
        {
            return ((short)(buffer.getByte(offset + 40) & 0xFF));
        }

        public OrderType orderType()
        {
            return OrderType.get(((short)(buffer.getByte(offset + 40) & 0xFF)));
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

        public short timeInForceRaw()
        {
            return ((short)(buffer.getByte(offset + 41) & 0xFF));
        }

        public TimeInForce timeInForce()
        {
            return TimeInForce.get(((short)(buffer.getByte(offset + 41) & 0xFF)));
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

        public long price()
        {
            return buffer.getLong(offset + 42, BYTE_ORDER);
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

        public long stopPrice()
        {
            return buffer.getLong(offset + 50, BYTE_ORDER);
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

        public long quantity()
        {
            return buffer.getLong(offset + 58, BYTE_ORDER);
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

        public long filledQty()
        {
            return buffer.getLong(offset + 66, BYTE_ORDER);
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

        public long remainingQty()
        {
            return buffer.getLong(offset + 74, BYTE_ORDER);
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

        public long lastPrice()
        {
            return buffer.getLong(offset + 82, BYTE_ORDER);
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

        public long lastQty()
        {
            return buffer.getLong(offset + 90, BYTE_ORDER);
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

        public long tradeId()
        {
            return buffer.getLong(offset + 98, BYTE_ORDER);
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

        public long meOrderId()
        {
            return buffer.getLong(offset + 106, BYTE_ORDER);
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

        public int holdAssetId()
        {
            return buffer.getInt(offset + 114, BYTE_ORDER);
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

        public long holdAmount()
        {
            return buffer.getLong(offset + 118, BYTE_ORDER);
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

        public long holdRemaining()
        {
            return buffer.getLong(offset + 126, BYTE_ORDER);
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

        public long clientOrderIdHigh()
        {
            return buffer.getLong(offset + 134, BYTE_ORDER);
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

        public long clientOrderIdLow()
        {
            return buffer.getLong(offset + 142, BYTE_ORDER);
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

        public long createdAtMs()
        {
            return buffer.getLong(offset + 150, BYTE_ORDER);
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

        public long updatedAtMs()
        {
            return buffer.getLong(offset + 158, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("eventSeq=");
            builder.append(this.eventSeq());
            builder.append('|');
            builder.append("logPosition=");
            builder.append(this.logPosition());
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
            builder.append("eventKind=");
            builder.append(this.eventKind());
            builder.append('|');
            builder.append("status=");
            builder.append(this.status());
            builder.append('|');
            builder.append("reason=");
            builder.append(this.reason());
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
            builder.append("stopPrice=");
            builder.append(this.stopPrice());
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
            builder.append("lastPrice=");
            builder.append(this.lastPrice());
            builder.append('|');
            builder.append("lastQty=");
            builder.append(this.lastQty());
            builder.append('|');
            builder.append("tradeId=");
            builder.append(this.tradeId());
            builder.append('|');
            builder.append("meOrderId=");
            builder.append(this.meOrderId());
            builder.append('|');
            builder.append("holdAssetId=");
            builder.append(this.holdAssetId());
            builder.append('|');
            builder.append("holdAmount=");
            builder.append(this.holdAmount());
            builder.append('|');
            builder.append("holdRemaining=");
            builder.append(this.holdRemaining());
            builder.append('|');
            builder.append("clientOrderIdHigh=");
            builder.append(this.clientOrderIdHigh());
            builder.append('|');
            builder.append("clientOrderIdLow=");
            builder.append(this.clientOrderIdLow());
            builder.append('|');
            builder.append("createdAtMs=");
            builder.append(this.createdAtMs());
            builder.append('|');
            builder.append("updatedAtMs=");
            builder.append(this.updatedAtMs());
            builder.append(')');

            return builder;
        }
        
        public EventsDecoder sbeSkip()
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

        final OrderEventBatchDecoder decoder = new OrderEventBatchDecoder();
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
        builder.append("[OrderEventBatch](sbeTemplateId=");
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
        builder.append("events=[");
        final int eventsOriginalOffset = events.offset;
        final int eventsOriginalIndex = events.index;
        final EventsDecoder events = this.events();
        if (events.count() > 0)
        {
            while (events.hasNext())
            {
                events.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        events.offset = eventsOriginalOffset;
        events.index = eventsOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public OrderEventBatchDecoder sbeSkip()
    {
        sbeRewind();
        EventsDecoder events = this.events();
        if (events.count() > 0)
        {
            while (events.hasNext())
            {
                events.next();
                events.sbeSkip();
            }
        }

        return this;
    }
}
