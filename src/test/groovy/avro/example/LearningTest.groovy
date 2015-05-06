package avro.example

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import spock.lang.Specification

import java.security.SecureRandom

/**
 * A learning test of the Avro library.
 */
class LearningTest extends Specification {

    def generator = new SecureRandom()

    String randomHexString() {
        Integer.toHexString( generator.nextInt( Integer.MAX_VALUE ) ).toUpperCase()
    }

    def 'exercise codec without using generated code'() {

        given: 'a parsed schema'
        def schema = new Schema.Parser().parse( getClass().classLoader.getResourceAsStream( 'user.avsc' ) )

        and: 'some records'
        def toEncode = (1..10).collect {
            def record = new GenericData.Record( schema )
            record.put( 'name', randomHexString() )
            record.put( 'favorite_number', generator.nextInt() )
            if ( generator.nextBoolean() ) {
                record.put( 'favorite_color', randomHexString() )
            }
            record
        }

        and: 'records are encoded to disk'
        def file = new File( 'build/users.avro' )
        def datumWriter = new GenericDatumWriter<GenericRecord>( schema )
        def dataFileWriter = new DataFileWriter<GenericRecord>( datumWriter )
        dataFileWriter.create( schema, file )
        toEncode.each {
            dataFileWriter.append( it )
        }
        dataFileWriter.flush()
        dataFileWriter.close()

        when: 'the records are decoded'
        def datumReader = new GenericDatumReader<GenericRecord>( schema )
        def dataFileReader = new DataFileReader<GenericRecord>( file, datumReader )
        List<GenericRecord> decoded = []
        while ( dataFileReader.hasNext() ) {
            decoded << dataFileReader.next()
        }
        dataFileReader.close()

        then: 'something'
        toEncode.size() == decoded.size()
        decoded.each {
            println( it )
        }
    }
}
