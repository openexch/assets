/* Generated SBE (Simple Binary Encoding) message codec. */
package com.openexchange.assets.infrastructure.generated;

import org.agrona.MutableDirectBuffer;


/**
 * Transport + log: asserts the session class (checked against the authenticator principal), sets the egress mask (replaces Subscribe) and the resumable mark (replaces the QueryFeedPosition peek), names the R03 reader kind (0 = not a retention reader) and the producer epoch. Reply AdminAck + ORIGIN AdmissionState.
 */
@SuppressWarnings("all")
public final class RegisterSessionEncoder
{
    public static final int BLOCK_LENGTH = 31;
    public static final int TEMPLATE_ID = 80;
    public static final int SCHEMA_ID = 2;
    public static final int SCHEMA_VERSION = 7;
    public static final String SEMANTIC_VERSION = "0.7";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RegisterSessionEncoder parentMessage = this;
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

    public RegisterSessionEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RegisterSessionEncoder wrapAndApplyHeader(
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

    public RegisterSessionEncoder correlationId(final long value)
    {
        buffer.putLong(offset + 0, value, BYTE_ORDER);
        return this;
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

    public RegisterSessionEncoder sessionClass(final SessionClass value)
    {
        buffer.putByte(offset + 8, (byte)value.value());
        return this;
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

    private final EgressChannelsEncoder channels = new EgressChannelsEncoder();

    public EgressChannelsEncoder channels()
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

    public RegisterSessionEncoder readerKind(final short value)
    {
        buffer.putByte(offset + 13, (byte)value);
        return this;
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

    public RegisterSessionEncoder resumable(final BoolFlag value)
    {
        buffer.putByte(offset + 14, (byte)value.value());
        return this;
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

    public RegisterSessionEncoder identity(final long value)
    {
        buffer.putLong(offset + 15, value, BYTE_ORDER);
        return this;
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

    public RegisterSessionEncoder sourceEpoch(final long value)
    {
        buffer.putLong(offset + 23, value, BYTE_ORDER);
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

        final RegisterSessionDecoder decoder = new RegisterSessionDecoder();
        decoder.wrap(buffer, offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
