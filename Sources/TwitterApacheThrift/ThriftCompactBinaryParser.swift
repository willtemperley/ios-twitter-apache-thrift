// for.swift
// TwitterApacheThrift
//
// Created by Will Temperley on 19/08/2025. All rights reserved.
// Copyright 2025 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

import CoreFoundation
import Foundation
import BinaryParsing

extension ThriftStruct {
    
    @_lifetime(&input)
    @inlinable
    init(parsing input: inout ParserSpan, index: Int?) throws {
        
        var fields: [Int: ThriftValue] = [:]
        var nextField = try ThriftStruct.readFieldMetadata(previousId: 0, parsing: &input)
        while nextField.type != .stop, let id = nextField.id {
            let value = try ThriftObject(parsing: &input, index: id, type: nextField.type, isCollection: false)
            fields[id] = ThriftValue(index: id, type: nextField.type, data: value)
            nextField = try ThriftStruct.readFieldMetadata(previousId: id, parsing: &input)
        }
        self = Self(index: index, fields: fields)
    }
    
    /// Reads a the field metadata from the thrift. This type is equivalent to a Dictionary
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The field thrift type. The thrift id if it is not a ThriftType.stop.
    @_lifetime(&input)
    @inlinable
    static func readFieldMetadata(previousId: Int, parsing input: inout ParserSpan) throws -> (type: ThriftType, id: Int?) {
        let binary = try UInt8(parsing: &input)
        if binary == 0 {
            return (.stop, nil)
        }
        let fieldIdDelta = UInt8((binary & 0xF0) >> 4)
        let fieldType = UInt8(binary & 0x0F)
        let type = try ThriftType(compactValue: fieldType)
        
        if fieldIdDelta == 0 {
            let val = try Int16(parsingLittleEndian: &input)
            let fieldId = Int(Int16(zigZag: val))
            return (type, fieldId)
        }
        return (type, Int(fieldIdDelta) + previousId)
    }
}

extension ThriftObject {
    
    @_lifetime(&input)
    @inlinable
    init(parsing input: inout ParserSpan, index: Int?, type: ThriftType, isCollection: Bool = false) throws {
        
        switch type {
        case .void: //Void is boolean true in compact thrift when used as a field.
            self = isCollection ? .stop : .data(Data([1]))
        case .bool: //bool is boolean false in compact thrift when used as a field but is one byte when used in a collection
            self = isCollection ? .data(try Data([UInt8(parsing: &input)])) : .data(Data([0]))
        case .byte:
            self = .data(try Data([UInt8(parsing: &input)]))
        case .double:
            self = .data(try Data([UInt8](parsing: &input, byteCount: 8)))
        case .int64, .int16, .int32:
            self = .data(try Data(unsignedLEBBytes: &input))
        case .string:
            self = .data(try Data(parsingLV: &input))
        case .structure:
            self = .struct(try ThriftStruct(parsing: &input, index: index))
        case .map:
            var values: [ThriftKeyedCollection.Value] = []
            let metadata = try ThriftObject.readMapMetadata(parsing: &input)
            for _ in 0..<metadata.size {
                let key = try Self(parsing: &input, index: nil, type: metadata.keyType, isCollection: true)
                let value = try Self(parsing: &input, index: nil, type: metadata.valueType, isCollection: true)
                values.append(.init(key: key, value: value))
            }
            self = .keyedCollection(ThriftKeyedCollection(index: index,
                                                          count: metadata.size,
                                                          keyType:metadata.keyType,
                                                          elementType: metadata.valueType,
                                                          value: values))
        case .list, .set:
            var values: [ThriftObject] = []
            let metadata = try ThriftObject.readListMetadata(parsing: &input)
            for _ in 0..<metadata.size {
                let value = try Self(parsing: &input, index: nil, type: metadata.elementType, isCollection: true)
                values.append(value)
            }
            self = .unkeyedCollection(ThriftUnkeyedCollection(index: index, count: metadata.size, elementType: type, value: values))
        default:
            self = .stop
        }
    }
    
    /// Reads a map object metadata from the thrift. This type is equivalent to a Dictionary
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The type of key as a thrift type. The type of values as a thrift type. The amount of elements.
    @_lifetime(&input)
    @inlinable
    static func readMapMetadata(parsing input: inout ParserSpan) throws -> (keyType: ThriftType, valueType: ThriftType, size: Int) {
        let binary = try UInt8(parsing: &input)
        if binary == 0 {
            //Empty map
            return (.stop, .stop, 0)
        }
        
        var span = try input.seeking(toRelativeOffset: binary)
        let size = try UInt32(parsingLEB128: &span)
        
        let types = try UInt8(parsing: &input)
        let keyType = UInt8(types >> 4) & 0x0F
        let elementType = UInt8(types & 0x0F)
        
        return (try ThriftType(compactValue: keyType), try ThriftType(compactValue: elementType), Int(size))
    }
    
    /// Reads a list object metadata from the thrift. This type is equivalent to a Array
    /// - Throws: ThriftDecoderError.readBufferOverflow when trying to read outside the range of data
    /// - Returns: The type of values as a thrift type. The amount of elements.
    @_lifetime(&input)
    @inlinable
    static func readListMetadata(parsing input: inout ParserSpan) throws -> (elementType: ThriftType, size: Int) {
        let binary = try UInt8(parsing: &input)
        let compactSize = UInt8(binary >> 4) & 0x0F
        let elementType = UInt8(binary & 0x0F)
        let type = try ThriftType(compactValue: elementType)
        
        //If size is 15 (1111) then it uses a different format
        if compactSize == 0b1111 {
            let size: UInt = try .init(parsingLEB128: &input)
            return (type, Int(size))
        }
        
        return (type, Int(compactSize))
    }
}

extension Data {
    
    @_lifetime(&input)
    @inlinable
    init(parsingLV input: inout ParserSpan) throws {
        let len = try UInt32(parsingLEB128: &input)
        let bytes: [UInt8] = try .init(parsing: &input, byteCount: Int(len))
        self = Data(bytes)
    }
    
    @_lifetime(&input)
    @inlinable
    init(unsignedLEBBytes input: inout ParserSpan) throws  {
        var bytes: [UInt8] = []
        while true {
            let byte = try UInt8(parsing: &input)
            bytes.append(byte)
            if (byte & 0x80) == 0 {
                break
            }
        }
        self = Data(bytes)
    }
}

/// A class for reading values from thrift data
struct ThriftCompactBinaryParser {
    
    @_lifetime(&input)
    @inlinable
    func readThrift(type: ThriftType, parsing input: inout ParserSpan) throws -> ThriftObject {
        return try readValue(parsing: &input, index: nil, type: type)
    }
    
    @_lifetime(&input)
    @inlinable
    func readStruct(parsing input: inout ParserSpan, index: Int?) throws -> ThriftStruct {
        return try ThriftStruct(parsing: &input, index: index)
    }
    
    @_lifetime(&input)
    @inlinable
    func readValue(parsing input: inout ParserSpan, index: Int?, type: ThriftType, isCollection: Bool = false) throws -> ThriftObject {
        return try .init(parsing: &input, index: index, type: type, isCollection: isCollection)
    }
}
