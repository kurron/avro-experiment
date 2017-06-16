package avro.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import groovy.transform.Canonical
import org.apache.avro.Schema
import spock.lang.Specification

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercise the various codec scenarios.
 */
class AvroUnitTest extends Specification {

    @Canonical
    static class User100 {
        String name
    }

    static String randomString() {
        Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
    }

    static AvroSchema loadSchema( String schemaFile ) {
        def schemaStream = AvroUnitTest.class.classLoader.getResourceAsStream( schemaFile )
        def raw = new Schema.Parser().setValidate( true ).parse( schemaStream )
        new AvroSchema( raw )
    }

    void 'exercise various codec scenarios'() {

        given: 'a reader schema'
        def writerSchema = loadSchema( writerSchemaFile )

        and: 'a mapper'
        def mapper = new ObjectMapper( new AvroFactory() )

        and: 'a reader schema'
        def readerSchema = loadSchema( readerSchemaFile )

        and: 'an encoded instance'
        def original = new User100( name: randomString() )
        byte[] encoded = mapper.writer( writerSchema ).writeValueAsBytes( original )

        when: 'the instance are recreated'
        def decoded = mapper.readerFor( readerType ).with( readerSchema ).readValue( encoded )

        then: 'encoded and decoded match'
        decoded == original

        where:
        writerSchemaFile          | readerSchemaFile          | writerType    | readerType
        'schemas/user-1.0.0.json' | 'schemas/user-1.0.0.json' | User100.class | User100.class
    }
}
