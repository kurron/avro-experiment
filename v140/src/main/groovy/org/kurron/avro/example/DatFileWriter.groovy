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

    static final dataFileLocation = '../v140.bin'
    // due to rounding of seconds, we hard code the time
    static final now = LocalDateTime.now( Clock.fixed( Instant.EPOCH, ZoneId.systemDefault() ) )
    static final date = now.toLocalDate().toEpochDay() as int
    static final time = now.toLocalTime().toSecondOfDay() * 1000
    static final intToLong = now.toLocalDate().toEpochDay() as int
    static final stringToBytes = now.toLocalDate().toString().getBytes( 'UTF-8')
    static final bytesToString = LocalDateTime.now().toLocalDate().toString()
    static final String FIRST_NAME = 'firstname-v140'
    static final String LAST_NAME = 'lastname-v140'
    static final String USER_NAME = 'username-v140'
    static final boolean ACTIVE = true
    static final int ID = Integer.MAX_VALUE
    static final Gender GENDER = Gender.FEMALE
    static final promotionExample = PromotionExample.newBuilder()
            .setIntToLong( intToLong )
            .setStringToBytes( ByteBuffer.wrap( stringToBytes ) )
            .setBytesToString( bytesToString )
            .build()
    static final String COMMENT = 'Reset password v140'
    static final String SESSION_KEY = 'May'
    static final int SESSION_VALUE = 140

    static void main(String[] args) {
        def encoded = User.newBuilder().setFirstname(FIRST_NAME)
                .setLastname(LAST_NAME)
                .setUsername(USER_NAME)
                .setActive(ACTIVE)
                .setId(ID)
                .setAddedDate(date)
                .setAddedTime(time)
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
