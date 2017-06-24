package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.LocalDateTime

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v120/build/written.bin'
    static final dataFileLocation = 'build/written.bin'
    static final now = LocalDateTime.now()
    static final date = now.toLocalDate().toEpochDay() as int
    static final time = now.toLocalTime().toSecondOfDay() * 1000
    static final intToLong = now.toLocalDate().toEpochDay() as int
    static final stringToBytes = now.toLocalDate().toString()
    static final bytesToString = now.toLocalDate().toString().getBytes( 'UTF-8')

    def 'exercise codec'() {
        given: 'a fresh object'
        def promotionExample = PromotionExample.newBuilder()
                                               .setIntToLong( intToLong )
                                               .setStringToBytes( stringToBytes )
                                               .setBytesToString(ByteBuffer.wrap( bytesToString ) )
                                               .build()
        def encoded = User.newBuilder().setFirstname( 'firstname-v130' )
                                       .setLastname( 'lastname-v130' )
                                       .setUsername( 'username-v130' )
                                       .setActive( true )
                                       .setId( Integer.MAX_VALUE )
                                       .setAddedDate( date )
                                       .setAddedTime( time )
                                       .setGender( Gender.FEMALE )
                                       .setPromotionExample( promotionExample )
                                       .build()
        encoded.comments.add( 'Reset password v130' )
        encoded.sessions['May'] = 130

        and: 'a writer'
        def datumWriter = new SpecificDatumWriter<User>( User )
        def dataFileWriter = new DataFileWriter<User>( datumWriter )

        and: 'an object is encoded to disk'
        dataFileWriter.create( encoded.getSchema(), new File( dataFileLocation ) )
        dataFileWriter.append( encoded )
        dataFileWriter.flush()
        dataFileWriter.close()

        when: 'the object is decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(dataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the encoded and decoded match'
        encoded.firstname == decoded.firstname as String
        encoded.lastname == decoded.lastname as String
        encoded.username == decoded.username as String
        encoded.active == decoded.active
        encoded.id == decoded.id
        encoded.addedDate == decoded.addedDate
        encoded.addedTime == decoded.addedTime
        encoded.gender == decoded.gender
        encoded.comments == decoded.comments
        // there is a CharSet to String comparison issue with the keys
        encoded.sessions as String == decoded.sessions as String

        encoded.promotionExample.intToLong == decoded.promotionExample.intToLong
        encoded.promotionExample.stringToBytes == decoded.promotionExample.stringToBytes as String
        encoded.promotionExample.bytesToString == decoded.promotionExample.bytesToString
    }

    def 'exercise forwards compatibility'() {
        when: 'an object decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(previousVersionDataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the decoded attributes make sense'
        'firstname-v120' == decoded.firstname as String
        'lastname-v120' == decoded.lastname as String
        'username-v120' == decoded.username as String
        true == decoded.active
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
