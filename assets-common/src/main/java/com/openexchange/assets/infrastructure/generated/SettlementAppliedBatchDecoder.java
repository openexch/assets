/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * A run of SettlementApplied events (id=13), in emission order
 */
@SuppressWarnings("all")
public final class SettlementAppliedBatchDecoder
{
    public static final int BLOCK_LENGTH = 0;
    public static final int TEMPLATE_ID = 24;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final SettlementAppliedBatchDecoder parentMessage = this;
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

    public SettlementAppliedBatchDecoder wrap(
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

    public SettlementAppliedBatchDecoder wrapAndApplyHeader(
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

    public SettlementAppliedBatchDecoder sbeRewind()
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

    private final SettlementsDecoder settlements = new SettlementsDecoder(this);

    public static long settlementsDecoderId()
    {
        return 10;
    }

    public static int settlementsDecoderSinceVersion()
    {
        return 0;
    }

    public SettlementsDecoder settlements()
    {
        settlements.wrap(buffer);
        return settlements;
    }

    public static final class SettlementsDecoder
        implements Iterable<SettlementsDecoder>, java.util.Iterator<SettlementsDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final SettlementAppliedBatchDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        SettlementsDecoder(final SettlementAppliedBatchDecoder parentMessage)
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

        public SettlementsDecoder next()
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
            return 24;
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

        public java.util.Iterator<SettlementsDecoder> iterator()
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

        public static int tradeIdId()
        {
            return 1;
        }

        public static int tradeIdSinceVersion()
        {
            return 0;
        }

        public static int tradeIdEncodingOffset()
        {
            return 0;
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
            return buffer.getLong(offset + 0, BYTE_ORDER);
        }


        public static int buyerUserIdId()
        {
            return 2;
        }

        public static int buyerUserIdSinceVersion()
        {
            return 0;
        }

        public static int buyerUserIdEncodingOffset()
        {
            return 8;
        }

        public static int buyerUserIdEncodingLength()
        {
            return 8;
        }

        public static String buyerUserIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long buyerUserIdNullValue()
        {
            return -9223372036854775808L;
        }

        public static long buyerUserIdMinValue()
        {
            return -9223372036854775807L;
        }

        public static long buyerUserIdMaxValue()
        {
            return 9223372036854775807L;
        }

        public long buyerUserId()
        {
            return buffer.getLong(offset + 8, BYTE_ORDER);
        }


        public static int sellerUserIdId()
        {
            return 3;
        }

        public static int sellerUserIdSinceVersion()
        {
            return 0;
        }

        public static int sellerUserIdEncodingOffset()
        {
            return 16;
        }

        public static int sellerUserIdEncodingLength()
        {
            return 8;
        }

        public static String sellerUserIdMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static long sellerUserIdNullValue()
        {
            return -9223372036854775808L;
        }

        public static long sellerUserIdMinValue()
        {
            return -9223372036854775807L;
        }

        public static long sellerUserIdMaxValue()
        {
            return 9223372036854775807L;
        }

        public long sellerUserId()
        {
            return buffer.getLong(offset + 16, BYTE_ORDER);
        }


        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("tradeId=");
            builder.append(this.tradeId());
            builder.append('|');
            builder.append("buyerUserId=");
            builder.append(this.buyerUserId());
            builder.append('|');
            builder.append("sellerUserId=");
            builder.append(this.sellerUserId());
            builder.append(')');

            return builder;
        }
        
        public SettlementsDecoder sbeSkip()
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

        final SettlementAppliedBatchDecoder decoder = new SettlementAppliedBatchDecoder();
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
        builder.append("[SettlementAppliedBatch](sbeTemplateId=");
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
        builder.append("settlements=[");
        final int settlementsOriginalOffset = settlements.offset;
        final int settlementsOriginalIndex = settlements.index;
        final SettlementsDecoder settlements = this.settlements();
        if (settlements.count() > 0)
        {
            while (settlements.hasNext())
            {
                settlements.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        settlements.offset = settlementsOriginalOffset;
        settlements.index = settlementsOriginalIndex;
        builder.append(']');

        limit(originalLimit);

        return builder;
    }
    
    public SettlementAppliedBatchDecoder sbeSkip()
    {
        sbeRewind();
        SettlementsDecoder settlements = this.settlements();
        if (settlements.count() > 0)
        {
            while (settlements.hasNext())
            {
                settlements.next();
                settlements.sbeSkip();
            }
        }

        return this;
    }
}
