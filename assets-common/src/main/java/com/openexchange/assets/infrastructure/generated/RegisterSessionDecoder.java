/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.DirectBuffer;


/**
 * Transport + log: asserts the session class (checked against the authenticator principal), sets the egress mask (replaces Subscribe) and the resumable mark (replaces the QueryFeedPosition peek), names the R03 reader kind (0 = not a retention reader) and the producer epoch. Reply AdminAck + ORIGIN AdmissionState.
 */
@SuppressWarnings("all")
public final class RegisterSessionDecoder
{
    public static final int BLOCK_LENGTH = 31;
    public static final int TEMPLATE_ID = 80;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 6;
    public static final String SEMANTIC_VERSION = "0.6";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RegisterSessionDecoder parentMessage = this;
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

    public RegisterSessionDecoder wrap(
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

    public RegisterSessionDecoder wrapAndApplyHeader(
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

    public RegisterSessionDecoder sbeRewind()
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


    public static int sessionClassId()
    {
        return 2;
    }

    public static int sessionClassSinceVersion()
    {
        return 0;
    }

    public static int sessionClassEncodingOffset()
    {
        return 8;
    }

    public static int sessionClassEncodingLength()
    {
        return 1;
    }

    public static String sessionClassMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short sessionClassRaw()
    {
        return ((short)(buffer.getByte(offset + 8) & 0xFF));
    }

    public SessionClass sessionClass()
    {
        return SessionClass.get(((short)(buffer.getByte(offset + 8) & 0xFF)));
    }


    public static int channelsId()
    {
        return 3;
    }

    public static int channelsSinceVersion()
    {
        return 0;
    }

    public static int channelsEncodingOffset()
    {
        return 9;
    }

    public static int channelsEncodingLength()
    {
        return 4;
    }

    public static String channelsMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    private final EgressChannelsDecoder channels = new EgressChannelsDecoder();

    public EgressChannelsDecoder channels()
    {
        channels.wrap(buffer, offset + 9);
        return channels;
    }

    public static int readerKindId()
    {
        return 4;
    }

    public static int readerKindSinceVersion()
    {
        return 0;
    }

    public static int readerKindEncodingOffset()
    {
        return 13;
    }

    public static int readerKindEncodingLength()
    {
        return 1;
    }

    public static String readerKindMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short readerKindNullValue()
    {
        return (short)255;
    }

    public static short readerKindMinValue()
    {
        return (short)0;
    }

    public static short readerKindMaxValue()
    {
        return (short)254;
    }

    public short readerKind()
    {
        return ((short)(buffer.getByte(offset + 13) & 0xFF));
    }


    public static int resumableId()
    {
        return 5;
    }

    public static int resumableSinceVersion()
    {
        return 0;
    }

    public static int resumableEncodingOffset()
    {
        return 14;
    }

    public static int resumableEncodingLength()
    {
        return 1;
    }

    public static String resumableMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short resumableRaw()
    {
        return ((short)(buffer.getByte(offset + 14) & 0xFF));
    }

    public BoolFlag resumable()
    {
        return BoolFlag.get(((short)(buffer.getByte(offset + 14) & 0xFF)));
    }


    public static int identityId()
    {
        return 6;
    }

    public static int identitySinceVersion()
    {
        return 0;
    }

    public static int identityEncodingOffset()
    {
        return 15;
    }

    public static int identityEncodingLength()
    {
        return 8;
    }

    public static String identityMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long identityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long identityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long identityMaxValue()
    {
        return 9223372036854775807L;
    }

    public long identity()
    {
        return buffer.getLong(offset + 15, BYTE_ORDER);
    }


    public static int sourceEpochId()
    {
        return 7;
    }

    public static int sourceEpochSinceVersion()
    {
        return 0;
    }

    public static int sourceEpochEncodingOffset()
    {
        return 23;
    }

    public static int sourceEpochEncodingLength()
    {
        return 8;
    }

    public static String sourceEpochMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long sourceEpochNullValue()
    {
        return -9223372036854775808L;
    }

    public static long sourceEpochMinValue()
    {
        return -9223372036854775807L;
    }

    public static long sourceEpochMaxValue()
    {
        return 9223372036854775807L;
    }

    public long sourceEpoch()
    {
        return buffer.getLong(offset + 23, BYTE_ORDER);
    }


    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final RegisterSessionDecoder decoder = new RegisterSessionDecoder();
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
        builder.append("[RegisterSession](sbeTemplateId=");
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
        builder.append("sessionClass=");
        builder.append(this.sessionClass());
        builder.append('|');
        builder.append("channels=");
        final EgressChannelsDecoder channels = this.channels();
        if (null != channels)
        {
            channels.appendTo(builder);
        }
        else
        {
            builder.append("null");
        }
        builder.append('|');
        builder.append("readerKind=");
        builder.append(this.readerKind());
        builder.append('|');
        builder.append("resumable=");
        builder.append(this.resumable());
        builder.append('|');
        builder.append("identity=");
        builder.append(this.identity());
        builder.append('|');
        builder.append("sourceEpoch=");
        builder.append(this.sourceEpoch());

        limit(originalLimit);

        return builder;
    }
    
    public RegisterSessionDecoder sbeSkip()
    {
        sbeRewind();

        return this;
    }
}
