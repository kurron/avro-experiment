package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.util.Utf8
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.LocalDateTime

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v130/build/written.bin'
    static final dataFileLocation = 'build/written.bin'
    static final now = LocalDateTime.now()
    static final date = now.toLocalDate().toEpochDay() as int
    static final time = now.toLocalTime().toSecondOfDay() * 1000
    static final intToLong = now.toLocalDate().toEpochDay() as int
    static final stringToBytes = now.toLocalDate().toString().getBytes( 'UTF-8')
    static final bytesToString = now.toLocalDate().toString()

    def 'exercise codec'() {
        given: 'a fresh object'
        def promotionExample = PromotionExample.newBuilder()
                                               .setIntToLong( intToLong )
                                               .setStringToBytes( ByteBuffer.wrap( stringToBytes ) )
                                               .setBytesToString( bytesToString )
                                               .build()
        def encoded = User.newBuilder().setFirstname( 'firstname-v140' )
                                       .setLastname( 'lastname-v140' )
                                       .setUsername( 'username-v140' )
                                       .setActive( true )
                                       .setId( Integer.MAX_VALUE )
                                       .setAddedDate( date )
                                       .setAddedTime( time )
                                       .setGender( Gender.FEMALE )
                                       .setPromotionExample( promotionExample )
                                       .build()
        encoded.comments.add( 'Reset password v140' )
        encoded.sessions['May'] = 140

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
        encoded.promotionExample.stringToBytes == decoded.promotionExample.stringToBytes
        encoded.promotionExample.bytesToString == decoded.promotionExample.bytesToString as String
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
        true == decoded.active
        Integer.MAX_VALUE == decoded.id
        date == decoded.addedDate
        0 < decoded.addedTime // can't know the exact millisecond saved
        Gender.FEMALE == decoded.gender
        'Reset password v130' == decoded.comments.first() as String
        // Avro's instance to not use String is annoying and make comparisons more difficult
        130 == decoded.sessions[ new Utf8('May') ]
        date == decoded.promotionExample.intToLong
        ByteBuffer.wrap( LocalDateTime.now().toLocalDate().toString().getBytes( 'UTF-8' ) ) == decoded.promotionExample.stringToBytes
        LocalDateTime.now().toLocalDate().toString() == decoded.promotionExample.bytesToString as String
    }
}
