package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import spock.lang.Specification

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v110/build/written.bin'
    static final dataFileLocation = 'build/written.bin'

    def 'exercise codec'() {
        given: 'a fresh object'
        def encoded = User.newBuilder().setFirstname( 'firstname-v120' )
                                       .setLastname( 'lastname-v120' )
                                       .setUsername( 'username-v120' ).build()

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
    }

    def 'exercise forwards compatibility'() {
        when: 'an object decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(previousVersionDataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the decoded attributes make sense'
        'name-v110' == decoded.firstname as String
        'defaulted v120 lastname' == decoded.lastname as String
        'username-v110' == decoded.username as String
    }
}
