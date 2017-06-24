package org.kurron.avro.example

import org.apache.avro.AvroTypeException
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import spock.lang.Specification

import java.time.LocalDateTime

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v140/build/written.bin'
    static final dataFileLocation = 'build/written.bin'
    static final now = LocalDateTime.now()
    static final date = now.toLocalDate().toEpochDay() as int
    static final time = now.toLocalTime().toSecondOfDay() * 1000

    def 'exercise codec'() {
        given: 'a fresh object'
        def encoded = User.newBuilder().setFirstname( 'firstname-v200' )
                                       .setLastname( 'lastname-v200' )
                                       .setUsername( 'username-v200' )
                                       .setActive( true )
                                       .setId( Integer.MAX_VALUE )
                                       .setAddedDate( date )
                                       .setAddedTime( time )
                                       .setGender( Gender.FEMALE )
                                       .setBreakingChange( 'breakingChange-v200' )
                                       .build()
        encoded.comments.add( 'Reset password v200' )
        encoded.sessions['May'] = 200

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
    }

    def 'exercise forwards compatibility'() {
        when: 'an object decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(previousVersionDataFileLocation), userDatumReader)
        dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'Avro complains about a missing required field'
        def error = thrown( AvroTypeException )
        error.message.contains( 'missing required field breakingChange' )
    }
}
