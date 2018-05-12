/**
 *
 * Author: Lukas Reichart on 3/9/15.
 * Purpose: Skipper adapter ( used by the sails.js framework )
 * License: MIT
 * Copyright Lukas Reichart @Antum 2015
 */

var streamSpy = require('stream_spy');
var path = require('path');
var Writable = require('stream').Writable;
var concat = require('concat-stream');
var azure = require( 'azure-storage');
var BlobConstants = require('azure-storage/lib/common/common.core').Constants.BlobConstants;
var _ = require( 'lodash' );
var mime = require( 'mime' );

module.exports = function SkipperAzure( globalOptions ) {
  globalOptions = globalOptions || {};

  var blobService = azure.createBlobService();
  var adapter;
  adapter = {

      read: function( fd, cb ) {
          var prefix = fd;
          var res;

          blobService.doesContainerExist( globalOptions.container, prefix, function( err, result ) {
              console.log("doesContainerExist", err);
              if ( err ) return cb( err );
              if (result){
                  console.log("doesContainerExist:result", result);
                  response(result);
              } else {
                  blobService.createContainer( globalOptions.container, prefix, function( err, result ) {
                      console.log("createContainer", err);
                      if ( err ) return cb( err );
                      console.log("createContainer:result", result);
                      response(result);
                  });
              }

              function response(result){
                blobService.createReadStream(globalOptions.container, prefix, res, function (err, result, response) {
                  if (err) { return cb(err); }
                }).pipe(concat(function (data) {
                  return cb(null, data);
                }));
              }
          });
      },

    rm: function( fd, cb ) {
      blobService.deleteBlobIfExists( globalOptions.container, fd, function( err, result, response ){
        if( err ) {
          return cb( err );
        }

        // construct response
        cb( null, {
          filename: fd,
          success: true,
          extra: response
        });
      });
    },

    ls: function( dirname, cb ) {
      if ( !dirname ) {
        dirname = '/';
      }

      var prefix = dirname;

      blobService.listBlobsSegmentedWithPrefix( globalOptions.container, prefix,
        null, function( err, result, response ) {
          if( err ) {
            return cb( err );
          }

          var data = _.map( result.entries, 'name');
          data = _.map(data, function snipPathPrefixes (thisPath) {
            thisPath = thisPath.replace(/^.*[\/]([^\/]*)$/, '$1');

            // Join the dirname with the filename
            thisPath = path.join(dirname, path.basename(thisPath));

            return thisPath;
          });
          cb( null, data );
        })
    },

    receive: AzureReceiver
  };

  return adapter;


  /**
   * A simple receiver for Skipper that writes Upstreams to Azure Blob Storage
   * to the configured container at the configured path.
   *
   * @param {Object} options
   * @returns {Stream.Writable}
   */
  function AzureReceiver( options ) {

    var self = this;

    options = options || {};
    options = _.defaults( options, globalOptions );
    
    var bytesRemaining = options.maxBytes || BlobConstants.MAX_BLOCK_BLOB_BLOCK_SIZE;

    if (bytesRemaining > BlobConstants.DEFAULT_SINGLE_BLOB_PUT_THRESHOLD_IN_BYTES * 50000) {
      throw new Error('Upload exceeds the size limitation. Max size is ' + BlobConstants.DEFAULT_SINGLE_BLOB_PUT_THRESHOLD_IN_BYTES * 50000 + ' but the current size is ' + bytesRemaining);
    }

    var receiver = Writable({
      objectMode: true
    });

    receiver.once( 'error', function( err ) {
      console.log( 'ERROR ON RECEIVER :: ', err );
    });

    receiver._write = function onFile( newFile, encoding, done ) {
      var startedAt = new Date();

      newFile.once( 'error', function( err ) {
        console.log( ('ERROR ON file read stream in receiver (%s) :: ', newFile.filename, err ).red );
      });

      var headers = options.headers || {};

      // Lookup content type with mime if not set
      if ( typeof headers['content-type'] === 'undefined' ) {
        headers['content-type'] = mime.getType( newFile.fd );
      }

      var uploadOptions = {
        contentType: headers['content-type']
      };

      var abortUpload = function () {
        var err = new Error('E_EXCEEDS_UPLOAD_LIMIT');
        err.code = 'E_EXCEEDS_UPLOAD_LIMIT';
        receiver.emit('error', err);
      }

      if (bytesRemaining <= 0) {
        abortUpload();
        return;
      }
      
      /**
       * As per the convention of streaming a multipart upload, the stream does not beforehand know
       * how large the multipart upload will be, even if a Content-Length header is present.
       * To know the exact multipart file size, the ending boundary would have to be scanned, which
       * is exactly what we want to prevent. Our only option with Azure is to assume the max amount of 
       * bytes will be sent.
       * 
       * Azure saves blobs in different ways based on the file size. These constants can be found in the package 'azure-storage' for:
       * 
       * - blob: lib/common/util/constants.js, in BlobConstants.DEFAULT_SINGLE_BLOB_PUT_THRESHOLD_IN_BYTES, 32MB by default
       * - block: lib/common/util/constants.js, in BlobConstants.MAX_BLOCK_BLOB_BLOCK_SIZE, 100MB by default
       * 
       * Since we don't know the stream size, we have to assume it will be options.maxBytes long.
       * We can upload many of these files in one upload, but the remaining bytes should be calculated after each
       * streamed chunk upload, to be able to refuse uploading past the limit of maxBytes.
       * 
       * This is why we tell Azure to expect 'byteLength', which is the remaining bytes from multiple succesful uploads.
       */
      var byteLength = bytesRemaining;

      newFile.on('data', function (chunk) {
        bytesRemaining -= chunk.length;
      });

      console.info('stream length: ', bytesRemaining);
      var uploader = blobService.createBlockBlobFromStream(
        options.container,
        newFile.fd,
        newFile, 
        byteLength,
        uploadOptions,
        function( err, result, response ) {
          if( err ) {
            console.log( ('Receiver: Error writing ' + newFile.filename + ' :: Cancelling upload and cleaning up already-written bytes ... ' ).red );
            receiver.emit( 'error', err );
            return;
          }

          function callback (err, result, response) {
            if( err ) {
              console.log( ('Receiver: Error writing ' + newFile.filename + ' :: Cancelling upload and cleaning up already-written bytes ... ' ).red );
              receiver.emit( 'error', err );
              return;
            }

            if ( bytesRemaining < 0 ) {
              adapter.rm(newFile.fd, function (err, result) {
                if (err) {
                  receiver.emit(err);
                }
                abortUpload();
              });
              return;
            }
            newFile.byteCount = +result.contentLength;
            newFile.size = newFile.byteCount;
            newFile.extra = response;

            var endedAt = new Date();
            var duration = ( endedAt - startedAt ) / 1000;

            console.log( 'UPLOAD took ' + duration + ' seconds .. ' );

            receiver.emit('writefile', newFile);
            done();
          }
          
          blobService.getBlobProperties(options.container, newFile.fd, callback);
        });
    };
    return receiver;
  }
};


