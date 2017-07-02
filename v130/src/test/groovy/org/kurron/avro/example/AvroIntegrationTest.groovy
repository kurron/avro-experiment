package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.util.Utf8
import spock.lang.Specification

import java.nio.ByteBuffer

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v120.bin'

    def 'exercise codec'() {
        given: 'a data file'
        def dataFile = new File(DatFileWriter.dataFileLocation)

        when: 'the object is decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(dataFile, userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the encoded and decoded match'
        DatFileWriter.FIRST_NAME == decoded.firstname as String
        DatFileWriter.LAST_NAME == decoded.lastname as String
        DatFileWriter.USERNAME == decoded.username as String
        DatFileWriter.ACTIVE == decoded.active
        DatFileWriter.ID == decoded.id
        DatFileWriter.date == decoded.addedDate
        DatFileWriter.time == decoded.addedTime
        DatFileWriter.GENDER == decoded.gender
        DatFileWriter.COMMENT == decoded.comments.first() as String
        // there is a CharSet to String comparison issue with the keys
        DatFileWriter.SESSION_VALUE == decoded.sessions[new Utf8( DatFileWriter.SESSION_KEY )]

        DatFileWriter.intToLong == decoded.promotionExample.intToLong
        DatFileWriter.stringToBytes == decoded.promotionExample.stringToBytes as String
        ByteBuffer.wrap( DatFileWriter.bytesToString ) == decoded.promotionExample.bytesToString
    }

    def 'exercise backwards compatibility'() {
        when: 'an object decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(previousVersionDataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the decoded attributes make sense'
        'firstname-v120' == decoded.firstname as String
        'lastname-v120' == decoded.lastname as String
        'username-v120' == decoded.username as String
        decoded.active
        0 == decoded.id
        0 == decoded.addedDate
        0 == decoded.addedTime
        Gender.UNDECLARED == decoded.gender
        [] == decoded.comments
        [:] == decoded.sessions
        -1 == decoded.promotionExample.intToLong
        'defaulted v130 stringToBytes' == decoded.promotionExample.stringToBytes as String
        ByteBuffer.wrap( 'defaulted v130 bytesToString'.getBytes( 'UTF-8' ) ) == decoded.promotionExample.bytesToString
    }
}
