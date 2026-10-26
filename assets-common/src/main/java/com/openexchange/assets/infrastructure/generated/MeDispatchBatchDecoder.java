/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * A run of MeDispatch entries (cap 64).
 */
@SuppressWarnings("all")
public final class MeDispatchBatchDecoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 105;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final MeDispatchBatchDecoder parentMessage = this;
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

    public MeDispatchBatchDecoder wrap(
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

    public MeDispatchBatchDecoder wrapAndApplyHeader(
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

    public MeDispatchBatchDecoder sbeRewind()
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

    private final DispatchesDecoder dispatches = new DispatchesDecoder(this);

    public static long dispatchesDecoderId()
    {
        return 20;
    }

    public static int dispatchesDecoderSinceVersion()
    {
        return 0;
    }

    public DispatchesDecoder dispatches()
    {
        dispatches.wrap(buffer);
        return dispatches;
    }

    public static final class DispatchesDecoder
        implements Iterable<DispatchesDecoder>, java.util.Iterator<DispatchesDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final MeDispatchBatchDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        DispatchesDecoder(final MeDispatchBatchDecoder parentMessage)
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

        public DispatchesDecoder next()
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
            return 89;
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

        public java.util.Iterator<DispatchesDecoder> iterator()
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

        public static int dispatchSeqId()
        {
            return 1;
        }

        public static int dispatchSeqSinceVersion()
        {
            return 0;
        }

        public static int dispatchSeqEncodingOffset()
        {
            return 0;
        }

        public static int dispatchSeqEncodingLength()
        {
            return 8;
        }

        public static String dispatchSeqMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long dispatchSeqNullValue()
        {
            return -9223372036854775808L;
        }

        public static long dispatchSeqMinValue()
        {
            return -9223372036854775807L;
        }

        public static long dispatchSeqMaxValue()
        {
            return 9223372036854775807L;
        }

        public long dispatchSeq()
        {
            return buffer.getLong(offset + 0, BYTE_ORDER);
        }


        public static int commandIdHighId()
        {
            return 2;
        }

        public static int commandIdHighSinceVersion()
        {
            return 0;
        }

        public static int commandIdHighEncodingOffset()
        {
            return 8;
        }

        public static int commandIdHighEncodingLength()
        {
            return 8;
        }

        public static String commandIdHighMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long commandIdHighNullValue()
        {
            return -9223372036854775808L;
        }

        public static long commandIdHighMinValue()
        {
            return -9223372036854775807L;
        }

        public static long commandIdHighMaxValue()
        {
            return 9223372036854775807L;
        }

        public long commandIdHigh()
        {
            return buffer.getLong(offset + 8, BYTE_ORDER);
        }


        public static int commandIdLowId()
        {
            return 3;
        }

        public static int commandIdLowSinceVersion()
        {
            return 0;
        }

        public static int commandIdLowEncodingOffset()
        {
            return 16;
        }

        public static int commandIdLowEncodingLength()
        {
            return 8;
        }

        public static String commandIdLowMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long commandIdLowNullValue()
        {
            return -9223372036854775808L;
        }

        public static long commandIdLowMinValue()
        {
            return -9223372036854775807L;
        }

        public static long commandIdLowMaxValue()
        {
            return 9223372036854775807L;
        }

        public long commandIdLow()
        {
            return buffer.getLong(offset + 16, BYTE_ORDER);
        }


        public static int kindId()
        {
            return 4;
        }

        public static int kindSinceVersion()
        {
            return 0;
        }

        public static int kindEncodingOffset()
        {
            return 24;
        }

        public static int kindEncodingLength()
        {
            return 1;
        }

        public static String kindMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public short kindRaw()
        {
            return ((short)(buffer.getByte(offset + 24) & 0xFF));
        }

        public DispatchKind kind()
        {
            return DispatchKind.get(((short)(buffer.getByte(offset + 24) & 0xFF)));
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
            return 25;
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
            return buffer.getLong(offset + 25, BYTE_ORDER);
        }


        public static int omsOrderIdId()
        {
            return 6;
        }

        public static int omsOrderIdSinceVersion()
        {
            return 0;
        }

        public static int omsOrderIdEncodingOffset()
        {
            return 33;
        }

        public static int omsOrderIdEncodingLength()
        {
            return 8;
        }

        public static String omsOrderIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long omsOrderIdNullValue()
        {
            return -9223372036854775808L;
        }

        public static long omsOrderIdMinValue()
        {
            return -9223372036854775807L;
        }

        public static long omsOrderIdMaxValue()
        {
            return 9223372036854775807L;
        }

        public long omsOrderId()
        {
            return buffer.getLong(offset + 33, BYTE_ORDER);
        }


        public static int oldOrderIdId()
        {
            return 7;
        }

        public static int oldOrderIdSinceVersion()
        {
            return 0;
        }

        public static int oldOrderIdEncodingOffset()
        {
            return 41;
        }

        public static int oldOrderIdEncodingLength()
        {
            return 8;
        }

        public static String oldOrderIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long oldOrderIdNullValue()
        {
            return -9223372036854775808L;
        }

        public static long oldOrderIdMinValue()
        {
            return -9223372036854775807L;
        }

        public static long oldOrderIdMaxValue()
        {
            return 9223372036854775807L;
        }

        public long oldOrderId()
        {
            return buffer.getLong(offset + 41, BYTE_ORDER);
        }


        public static int marketIdId()
        {
            return 8;
        }

        public static int marketIdSinceVersion()
        {
            return 0;
        }

        public static int marketIdEncodingOffset()
        {
            return 49;
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
            return buffer.getInt(offset + 49, BYTE_ORDER);
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

        public static short orderTypeNullValue()
        {
            return (short)255;
        }

        public static short orderTypeMinValue()
        {
            return (short)0;
        }

        public static short orderTypeMaxValue()
        {
            return (short)254;
        }

        public short orderType()
        {
            return ((short)(buffer.getByte(offset + 53) & 0xFF));
        }


        public static int sideId()
        {
            return 10;
        }

        public static int sideSinceVersion()
        {
            return 0;
        }

        public static int sideEncodingOffset()
        {
            return 54;
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

        public static short sideNullValue()
        {
            return (short)255;
        }

        public static short sideMinValue()
        {
            return (short)0;
        }

        public static short sideMaxValue()
        {
            return (short)254;
        }

        public short side()
        {
            return ((short)(buffer.getByte(offset + 54) & 0xFF));
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

        public long budget()
        {
            return buffer.getLong(offset + 71, BYTE_ORDER);
        }


        public static int timeInForceId()
        {
            return 14;
        }

        public static int timeInForceSinceVersion()
        {
            return 0;
        }

        public static int timeInForceEncodingOffset()
        {
            return 79;
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

        public static short timeInForceNullValue()
        {
            return (short)255;
        }

        public static short timeInForceMinValue()
        {
            return (short)0;
        }

        public static short timeInForceMaxValue()
        {
            return (short)254;
        }

        public short timeInForce()
        {
            return ((short)(buffer.getByte(offset + 79) & 0xFF));
        }


        public static int releaseScopeId()
        {
            return 15;
        }

        public static int releaseScopeSinceVersion()
        {
            return 7;
        }

        public static int releaseScopeEncodingOffset()
        {
            return 80;
        }

        public static int releaseScopeEncodingLength()
        {
            return 1;
        }

        public static String releaseScopeMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public short releaseScopeRaw()
        {
            if (parentMessage.actingVersion < 7)
            {
                return (short)255;
            }

            return ((short)(buffer.getByte(offset + 80) & 0xFF));
        }

        public ReleaseScope releaseScope()
        {
            if (parentMessage.actingVersion < 7)
            {
                return ReleaseScope.NULL_VAL;
            }

            return ReleaseScope.get(((short)(buffer.getByte(offset + 80) & 0xFF)));
        }


        public static int expectedOldRemainingId()
        {
            return 16;
        }

        public static int expectedOldRemainingSinceVersion()
        {
            return 7;
        }

        public static int expectedOldRemainingEncodingOffset()
        {
            return 81;
        }

        public static int expectedOldRemainingEncodingLength()
        {
            return 8;
        }

        public static String expectedOldRemainingMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long expectedOldRemainingNullValue()
        {
            return -9223372036854775808L;
        }

        public static long expectedOldRemainingMinValue()
        {
            return -9223372036854775807L;
        }

        public static long expectedOldRemainingMaxValue()
        {
            return 9223372036854775807L;
        }

        public long expectedOldRemaining()
        {
            if (parentMessage.actingVersion < 7)
            {
                return -9223372036854775808L;
            }

            return buffer.getLong(offset + 81, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("dispatchSeq=");
            builder.append(this.dispatchSeq());
            builder.append('|');
            builder.append("commandIdHigh=");
            builder.append(this.commandIdHigh());
            builder.append('|');
            builder.append("commandIdLow=");
            builder.append(this.commandIdLow());
            builder.append('|');
            builder.append("kind=");
            builder.append(this.kind());
            builder.append('|');
            builder.append("userId=");
            builder.append(this.userId());
            builder.append('|');
            builder.append("omsOrderId=");
            builder.append(this.omsOrderId());
            builder.append('|');
            builder.append("oldOrderId=");
            builder.append(this.oldOrderId());
            builder.append('|');
            builder.append("marketId=");
            builder.append(this.marketId());
            builder.append('|');
            builder.append("orderType=");
            builder.append(this.orderType());
            builder.append('|');
            builder.append("side=");
            builder.append(this.side());
            builder.append('|');
            builder.append("price=");
            builder.append(this.price());
            builder.append('|');
            builder.append("quantity=");
            builder.append(this.quantity());
            builder.append('|');
            builder.append("budget=");
            builder.append(this.budget());
            builder.append('|');
            builder.append("timeInForce=");
            builder.append(this.timeInForce());
            builder.append('|');
            builder.append("releaseScope=");
            builder.append(this.releaseScope());
            builder.append('|');
            builder.append("expectedOldRemaining=");
            builder.append(this.expectedOldRemaining());
            builder.append(')');

            return builder;
        }
        
        public DispatchesDecoder sbeSkip()
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

        final MeDispatchBatchDecoder decoder = new MeDispatchBatchDecoder();
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
        builder.append("[MeDispatchBatch](sbeTemplateId=");
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
        builder.append("dispatches=[");
        final int dispatchesOriginalOffset = dispatches.offset;
        final int dispatchesOriginalIndex = dispatches.index;
        final DispatchesDecoder dispatches = this.dispatches();
        if (dispatches.count() > 0)
        {
            while (dispatches.hasNext())
            {
                dispatches.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        dispatches.offset = dispatchesOriginalOffset;
        dispatches.index = dispatchesOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public MeDispatchBatchDecoder sbeSkip()
    {
        sbeRewind();
        DispatchesDecoder dispatches = this.dispatches();
        if (dispatches.count() > 0)
        {
            while (dispatches.hasNext())
            {
                dispatches.next();
                dispatches.sbeSkip();
            }
        }

        return this;
    }
}
