package org.kurron.avro.example

import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import spock.lang.Specification

/**
 * Exercises Avro codec.
 */
class AvroIntegrationTest extends Specification {

    static final previousVersionDataFileLocation = '../v100.bin'

    def 'exercise codec'() {
        given: 'a data file with an object in it'
        def datafile = new File(DatFileWriter.dataFileLocation)

        when: 'the object is decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(datafile, userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the encoded and decoded match'
        DatFileWriter.NAME == decoded.name as String
        DatFileWriter.USERNAME == decoded.username as String
    }

    def 'exercise forwards compatibility'() {
        when: 'an object decoded from disk'
        def userDatumReader = new SpecificDatumReader<User>(User)
        def dataFileReader = new DataFileReader<User>(new File(previousVersionDataFileLocation), userDatumReader)
        def decoded = dataFileReader.hasNext() ? dataFileReader.next( new User() ) : new User()

        then: 'the decoded attributes make sense'
        'name-v100' == decoded.name as String
        'defaulted v110 username' == decoded.username as String
    }
}
