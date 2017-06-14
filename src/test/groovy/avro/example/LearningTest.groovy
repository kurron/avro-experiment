package avro.example

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.springframework.boot.test.context.SpringBootTest
import spock.lang.Specification

import java.nio.ByteBuffer
import java.security.SecureRandom

/**
 * A learning test of the Avro library.
 */
@SpringBootTest
class LearningTest extends Specification {

    def generator = new SecureRandom()

    String randomHexString() {
        Integer.toHexString( generator.nextInt( Integer.MAX_VALUE ) ).toUpperCase()
    }

    def 'exercise codec without using generated code'() {

        given: 'a parsed schema'
        def schema = new Schema.Parser().parse( getClass().classLoader.getResourceAsStream( 'user.avsc' ) )

        and: 'some records'
        def toEncode = (1..20).collect {
            def record = new GenericData.Record( schema )
            record.put( 'nullValue', randomHexString() )
            record.put( 'booleanValue', generator.nextBoolean() )
            record.put( 'intValue', generator.nextInt() )
            record.put( 'longValue', generator.nextLong() )
            record.put( 'floatValue', generator.nextFloat() )
            record.put( 'doubleValue', generator.nextDouble() )
            byte[] buffer = new byte[2]
            generator.nextBytes( buffer )
            record.put( 'byteValue', ByteBuffer.wrap( buffer ) )
            record.put( 'stringValue', randomHexString() )
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
        //TODO: can we write to something other than disk?
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
