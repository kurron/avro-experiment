package org.kurron.avro.example

import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

import java.nio.ByteBuffer
import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

/**
 * Should run before the tests, outputting this schema's data to a file.
 */
class DatFileWriter {

    static final dataFileLocation = '../v130.bin'
    // due to rounding of seconds, we hard code the time
    static final now = LocalDateTime.now( Clock.fixed( Instant.EPOCH, ZoneId.systemDefault() ) )
    static final date = Math.toIntExact( now.toLocalDate().toEpochDay() )
    static final time = Math.toIntExact( now.toLocalTime().toSecondOfDay() )
    static final intToLong = Integer.MAX_VALUE
    static final stringToBytes = LocalDateTime.now().toLocalDate().toString()
    static final bytesToString = LocalDateTime.now().toLocalDate().toString().getBytes( 'UTF-8')
    public static final String FIRST_NAME = 'firstname-v130'
    public static final String LAST_NAME = 'lastname-v130'
    public static final String USERNAME = 'username-v130'
    public static final boolean ACTIVE = true
    public static final int ID = Integer.MAX_VALUE
    public static final String COMMENT = 'Reset password v130'
    public static final String SESSION_KEY = 'May'
    public static final int SESSION_VALUE = 130
    public static final Gender GENDER = Gender.FEMALE

    static void main(String[] args) {
        def promotionExample = PromotionExample.newBuilder()
                                               .setIntToLong( intToLong )
                                               .setStringToBytes( stringToBytes )
                                               .setBytesToString(ByteBuffer.wrap( bytesToString ) )
                                               .build()
        def encoded = User.newBuilder()
                          .setFirstname(FIRST_NAME)
                          .setLastname(LAST_NAME)
                          .setUsername(USERNAME)
                          .setActive(ACTIVE)
                          .setId(ID)
                          .setAddedDate( date )
                          .setAddedTime( time )
                          .setGender(GENDER)
                          .setPromotionExample( promotionExample )
                          .build()
        encoded.comments.add(COMMENT)
        encoded.sessions[SESSION_KEY] = SESSION_VALUE

        def datumWriter = new SpecificDatumWriter<User>( User )
        def dataFileWriter = new DataFileWriter<User>( datumWriter )

        dataFileWriter.create( encoded.getSchema(), new File( dataFileLocation ) )
        dataFileWriter.append( encoded )
        dataFileWriter.flush()
        dataFileWriter.close()
    }
}