package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import spock.lang.Specification

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final dataFileLocation = 'build/written.bin'

    static String randomString() {
        Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
    }

    def 'exercise codec'() {
        given: 'a fresh object'
        def encoded = User.newBuilder().setName( randomString() ).build()

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
        encoded == decoded
    }
}
