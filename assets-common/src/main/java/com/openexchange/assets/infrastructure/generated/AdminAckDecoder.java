/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * ORIGIN reply to every admin command and to RegisterSession (refTemplateId names it).
 */
@SuppressWarnings("all")
public final class AdminAckDecoder
{
    public static final int BLOCK_LENGTH = 16;
    public static final int TEMPLATE_ID = 110;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final AdminAckDecoder parentMessage = this;
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

    public AdminAckDecoder wrap(
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

    public AdminAckDecoder wrapAndApplyHeader(
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

    public AdminAckDecoder sbeRewind()
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


    public static int refTemplateIdId()
    {
        return 2;
    }

    public static int refTemplateIdSinceVersion()
    {
        return 0;
    }

    public static int refTemplateIdEncodingOffset()
    {
        return 8;
    }

    public static int refTemplateIdEncodingLength()
    {
        return 2;
    }

    public static String refTemplateIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int refTemplateIdNullValue()
    {
        return 65535;
    }

    public static int refTemplateIdMinValue()
    {
        return 0;
    }

    public static int refTemplateIdMaxValue()
    {
        return 65534;
    }

    public int refTemplateId()
    {
        return (buffer.getShort(offset + 8, BYTE_ORDER) & 0xFFFF);
    }


    public static int resultId()
    {
        return 3;
    }

    public static int resultSinceVersion()
    {
        return 0;
    }

    public static int resultEncodingOffset()
    {
        return 10;
    }

    public static int resultEncodingLength()
    {
        return 1;
    }

    public static String resultMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short resultRaw()
    {
        return ((short)(buffer.getByte(offset + 10) & 0xFF));
    }

    public AdminResult result()
    {
        return AdminResult.get(((short)(buffer.getByte(offset + 10) & 0xFF)));
    }


    public static int reasonId()
    {
        return 4;
    }

    public static int reasonSinceVersion()
    {
        return 0;
    }

    public static int reasonEncodingOffset()
    {
        return 11;
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

    public static short reasonNullValue()
    {
        return (short)255;
    }

    public static short reasonMinValue()
    {
        return (short)0;
    }

    public static short reasonMaxValue()
    {
        return (short)254;
    }

    public short reason()
    {
        return ((short)(buffer.getByte(offset + 11) & 0xFF));
    }


    public static int configVersionId()
    {
        return 5;
    }

    public static int configVersionSinceVersion()
    {
        return 0;
    }

    public static int configVersionEncodingOffset()
    {
        return 12;
    }

    public static int configVersionEncodingLength()
    {
        return 4;
    }

    public static String configVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long configVersionNullValue()
    {
        return 4294967295L;
    }

    public static long configVersionMinValue()
    {
        return 0L;
    }

    public static long configVersionMaxValue()
    {
        return 4294967294L;
    }

    public long configVersion()
    {
        return (buffer.getInt(offset + 12, BYTE_ORDER) & 0xFFFF_FFFFL);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final AdminAckDecoder decoder = new AdminAckDecoder();
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
        builder.append("[AdminAck](sbeTemplateId=");
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
        builder.append("refTemplateId=");
        builder.append(this.refTemplateId());
        builder.append('|');
        builder.append("result=");
        builder.append(this.result());
        builder.append('|');
        builder.append("reason=");
        builder.append(this.reason());
        builder.append('|');
        builder.append("configVersion=");
        builder.append(this.configVersion());

        limit(originalLimit);

        return builder;
    }
    
    public AdminAckDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
