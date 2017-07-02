package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import spock.lang.Specification

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v110.bin'

    def 'exercise codec'() {
        given: 'an encoded file'
        def dataFile = new File(DatFileWriter.dataFileLocation)

        when: 'the object is decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(dataFile, userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the encoded and decoded match'
        DatFileWriter.FIRST_NAME == decoded.firstname as String
        DatFileWriter.LAST_NAME == decoded.lastname as String
        DatFileWriter.USERNAME == decoded.username as String
    }

    def 'exercise backwards compatibility'() {
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
