package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.util.Utf8
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.LocalDateTime

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v130.bin'

    def 'exercise codec'() {
        given: 'a fresh object'

        when: 'the object is decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(DatFileWriter.dataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the encoded and decoded match'
        DatFileWriter.FIRST_NAME == decoded.firstname as String
        DatFileWriter.LAST_NAME == decoded.lastname as String
        DatFileWriter.USER_NAME == decoded.username as String
        DatFileWriter.ACTIVE == decoded.active
        DatFileWriter.ID == decoded.id
        DatFileWriter.date == decoded.addedDate
        DatFileWriter.time == decoded.addedTime
        DatFileWriter.GENDER == decoded.gender
        DatFileWriter.COMMENT == decoded.comments.first() as String
        // there is a CharSet to String comparison issue with the keys
        DatFileWriter.SESSION_VALUE == decoded.sessions[new Utf8( DatFileWriter.SESSION_KEY )]

        DatFileWriter.promotionExample.intToLong == decoded.promotionExample.intToLong
        DatFileWriter.promotionExample.stringToBytes == decoded.promotionExample.stringToBytes
        DatFileWriter.promotionExample.bytesToString == decoded.promotionExample.bytesToString as String
    }

    def 'exercise backwards compatibility'() {
        when: 'an object decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(previousVersionDataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the decoded attributes make sense'
        'firstname-v130' == decoded.firstname as String
        'lastname-v130' == decoded.lastname as String
        'username-v130' == decoded.username as String
        decoded.active
        Integer.MAX_VALUE == decoded.id
        DatFileWriter.date == decoded.addedDate
        0 < decoded.addedTime // can't know the exact millisecond saved
        Gender.FEMALE == decoded.gender
        'Reset password v130' == decoded.comments.first() as String
        // Avro's instance to not use String is annoying and make comparisons more difficult
        130 == decoded.sessions[ new Utf8('May') ]
        Integer.MAX_VALUE == decoded.promotionExample.intToLong
        ByteBuffer.wrap( LocalDateTime.now().toLocalDate().toString().getBytes( 'UTF-8' ) ) == decoded.promotionExample.stringToBytes
        decoded.promotionExample.bytesToString as String == LocalDateTime.now().toLocalDate().toString()
    }
}
