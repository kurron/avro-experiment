package avro.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import groovy.transform.Canonical
import org.apache.avro.Schema
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercise the various codec scenarios.
 */
class AvroUnitTest extends Specification {

    @Canonical
    static class User100 {
        String name
    }

    @Canonical
    static class User110 {
        String name
        String username
    }

    static String randomString() {
        Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
    }

    static AvroSchema loadSchema( String schemaFile ) {
        def schemaStream = AvroUnitTest.class.classLoader.getResourceAsStream( schemaFile )
        def raw = new Schema.Parser().setValidate( true ).parse( schemaStream )
        new AvroSchema( raw )
    }

    static def v100Builder = {
        new User100( name: randomString() )
    }

    static def v110Builder = {
        new User110( name: randomString(), username: randomString() )
    }

    static def v100tov100Expectation = { User100 writer, User100 reader ->
        writer.name == reader.name
    }

    static def v110tov100Expectation = { User110 writer, User100 reader ->
        writer.name == reader.name
        // reader doesn't care about username yet
    }

    @Unroll
    void '#description'() {

        given: 'a writer schema'
        def writerSchema = loadSchema( writerSchemaFile )

        and: 'a mapper'
        def mapper = new ObjectMapper( new AvroFactory() )

        and: 'a reader schema'
        def readerSchema = loadSchema( readerSchemaFile )

        and: 'an encoded instance'
        def original = writerClosure.call()
        byte[] encoded = mapper.writer( writerSchema ).writeValueAsBytes( original )

        when: 'the instance is decoded'
        def decoded = mapper.readerFor( readerType ).with( readerSchema ).readValue( encoded )

        then: 'encoded and decoded match'
        expectation.call( original, decoded )

        where:
        writerSchemaFile          | readerSchemaFile          | writerClosure | readerType    | description                    || expectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.0.0.json' | v100Builder   | User100.class | 'Reader matches writer'        || v100tov100Expectation
        'schemas/user-1.1.0.json' | 'schemas/user-1.0.0.json' | v110Builder   | User100.class | 'Writer adds additional field' || v110tov100Expectation
    }
}
