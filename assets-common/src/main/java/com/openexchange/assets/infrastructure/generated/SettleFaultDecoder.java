/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * A settle leg could not draw fully from its order hold (reconciler race / invariant breach): drawnFromAvailable was recovered from the user's available balance; uncovered could not be moved at all -- the counterparty was credited only what was debited, so conservation holds; nonzero uncovered demands operator action. MUST alert on this event.
 */
@SuppressWarnings("all")
public final class SettleFaultDecoder
{
    public static final int BLOCK_LENGTH = 44;
    public static final int TEMPLATE_ID = 21;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 2;
    public static final String SEMANTIC_VERSION = "0.2";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final SettleFaultDecoder parentMessage = this;
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

    public SettleFaultDecoder wrap(
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

    public SettleFaultDecoder wrapAndApplyHeader(
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

    public SettleFaultDecoder sbeRewind()
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


    public static int assetIdId()
    {
        return 4;
    }

    public static int assetIdSinceVersion()
    {
        return 0;
    }

    public static int assetIdEncodingOffset()
    {
        return 24;
    }

    public static int assetIdEncodingLength()
    {
        return 4;
    }

    public static String assetIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int assetIdNullValue()
    {
        return -2147483648;
    }

    public static int assetIdMinValue()
    {
        return -2147483647;
    }

    public static int assetIdMaxValue()
    {
        return 2147483647;
    }

    public int assetId()
    {
        return buffer.getInt(offset + 24, BYTE_ORDER);
    }


    public static int drawnFromAvailableId()
    {
        return 5;
    }

    public static int drawnFromAvailableSinceVersion()
    {
        return 0;
    }

    public static int drawnFromAvailableEncodingOffset()
    {
        return 28;
    }

    public static int drawnFromAvailableEncodingLength()
    {
        return 8;
    }

    public static String drawnFromAvailableMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long drawnFromAvailableNullValue()
    {
        return -9223372036854775808L;
    }

    public static long drawnFromAvailableMinValue()
    {
        return -9223372036854775807L;
    }

    public static long drawnFromAvailableMaxValue()
    {
        return 9223372036854775807L;
    }

    public long drawnFromAvailable()
    {
        return buffer.getLong(offset + 28, BYTE_ORDER);
    }


    public static int uncoveredId()
    {
        return 6;
    }

    public static int uncoveredSinceVersion()
    {
        return 0;
    }

    public static int uncoveredEncodingOffset()
    {
        return 36;
    }

    public static int uncoveredEncodingLength()
    {
        return 8;
    }

    public static String uncoveredMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long uncoveredNullValue()
    {
        return -9223372036854775808L;
    }

    public static long uncoveredMinValue()
    {
        return -9223372036854775807L;
    }

    public static long uncoveredMaxValue()
    {
        return 9223372036854775807L;
    }

    public long uncovered()
    {
        return buffer.getLong(offset + 36, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final SettleFaultDecoder decoder = new SettleFaultDecoder();
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
        builder.append("[SettleFault](sbeTemplateId=");
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
        builder.append("tradeId=");
        builder.append(this.tradeId());
        builder.append('|');
        builder.append("orderId=");
        builder.append(this.orderId());
        builder.append('|');
        builder.append("userId=");
        builder.append(this.userId());
        builder.append('|');
        builder.append("assetId=");
        builder.append(this.assetId());
        builder.append('|');
        builder.append("drawnFromAvailable=");
        builder.append(this.drawnFromAvailable());
        builder.append('|');
        builder.append("uncovered=");
        builder.append(this.uncovered());

        limit(originalLimit);

        return builder;
    }
    
    public SettleFaultDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
