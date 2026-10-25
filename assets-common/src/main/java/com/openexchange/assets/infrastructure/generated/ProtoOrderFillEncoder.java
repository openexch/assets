/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * RC0 spike ingress: a trade between two core orders (stands in for the ME journal feed)
 */
@SuppressWarnings("all")
public final class ProtoOrderFillEncoder
{
    public static final int BLOCK_LENGTH = 53;
    public static final int TEMPLATE_ID = 42;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 5;
    public static final String SEMANTIC_VERSION = "0.5";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final ProtoOrderFillEncoder parentMessage = this;
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

    public ProtoOrderFillEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ProtoOrderFillEncoder wrapAndApplyHeader(
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

    public ProtoOrderFillEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
    }


    public static int tradeIdId()
    {
        return 2;
    }

    public static int tradeIdSinceVersion()
    {
        return 0;
    }

    public static int tradeIdEncodingOffset()
    {
        return 8;
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

    public ProtoOrderFillEncoder tradeId(final long value)
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

    public ProtoOrderFillEncoder marketId(final int value)
    {
        buffer.putInt(offset + 16, value, BYTE_ORDER);
        return this;
    }


    public static int buyOrderIdId()
    {
        return 4;
    }

    public static int buyOrderIdSinceVersion()
    {
        return 0;
    }

    public static int buyOrderIdEncodingOffset()
    {
        return 20;
    }

    public static int buyOrderIdEncodingLength()
    {
        return 8;
    }

    public static String buyOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long buyOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long buyOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long buyOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ProtoOrderFillEncoder buyOrderId(final long value)
    {
        buffer.putLong(offset + 20, value, BYTE_ORDER);
        return this;
    }


    public static int sellOrderIdId()
    {
        return 5;
    }

    public static int sellOrderIdSinceVersion()
    {
        return 0;
    }

    public static int sellOrderIdEncodingOffset()
    {
        return 28;
    }

    public static int sellOrderIdEncodingLength()
    {
        return 8;
    }

    public static String sellOrderIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long sellOrderIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long sellOrderIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long sellOrderIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ProtoOrderFillEncoder sellOrderId(final long value)
    {
        buffer.putLong(offset + 28, value, BYTE_ORDER);
        return this;
    }


    public static int priceId()
    {
        return 6;
    }

    public static int priceSinceVersion()
    {
        return 0;
    }

    public static int priceEncodingOffset()
    {
        return 36;
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

    public ProtoOrderFillEncoder price(final long value)
    {
        buffer.putLong(offset + 36, value, BYTE_ORDER);
        return this;
    }


    public static int quantityId()
    {
        return 7;
    }

    public static int quantitySinceVersion()
    {
        return 0;
    }

    public static int quantityEncodingOffset()
    {
        return 44;
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

    public ProtoOrderFillEncoder quantity(final long value)
    {
        buffer.putLong(offset + 44, value, BYTE_ORDER);
        return this;
    }


    public static int takerIsBuyId()
    {
        return 8;
    }

    public static int takerIsBuySinceVersion()
    {
        return 0;
    }

    public static int takerIsBuyEncodingOffset()
    {
        return 52;
    }

    public static int takerIsBuyEncodingLength()
    {
        return 1;
    }

    public static String takerIsBuyMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public ProtoOrderFillEncoder takerIsBuy(final BoolFlag value)
    {
        buffer.putByte(offset + 52, (byte)value.value());
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

        final ProtoOrderFillDecoder decoder = new ProtoOrderFillDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
