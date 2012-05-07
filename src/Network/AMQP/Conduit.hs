module Network.AMQP.Conduit (
    -- * Sources, Sinks, and Conduits
      amqpConduit
    , amqpSink

    -- * Text.URI re-exports
    , URI
    , parseURI

    -- * Network.AMQP re-exports
    , ExchangeOpts(..)
    , QueueOpts(..)
    , newExchange
    , newQueue
    ) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Conduit
import Data.Maybe             (fromJust, fromMaybe)
import Data.List.Split        (splitOn)
import Text.URI               (URI(..), parseURI)
import Network.AMQP

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.Char8 as BL

data AMQPConn = AMQPConn
    { amqpConn     :: Connection
    , amqpChan     :: Channel
    , amqpQueue    :: ExchangeOpts
    , amqpExchange :: QueueOpts
    }

amqpConduit :: MonadResource m
            => URI
            -> ExchangeOpts
            -> QueueOpts
            -> Conduit BS.ByteString m BS.ByteString
amqpConduit uri exchange queue =
    conduitIO
    (connect uri exchange queue)
    disconnect
    (\conn bstr -> push (IOProducing [bstr]) conn bstr)
    (close [])

amqpSink :: MonadResource m
         => URI
         -> ExchangeOpts
         -> QueueOpts
         -> Sink BS.ByteString m ()
amqpSink uri exchange queue =
    sinkIO
    (connect uri exchange queue)
    disconnect
    (push IOProcessing)
    (close ())

--
-- Conduit Helpers
--

push :: MonadResource m => b -> AMQPConn -> BS.ByteString -> m b
push res conn bstr = do
    liftIO $ publish conn bstr
    return res

close :: MonadResource m => a -> AMQPConn -> m a
close res conn = do
    liftIO $ disconnect conn
    return res

--
-- Internal
--

connect :: URI -> ExchangeOpts -> QueueOpts -> IO AMQPConn
connect uri exchange queue = do
    conn <- openConnection' uri
    chan <- openChannel conn
    declareQueue chan queue
    declareExchange chan exchange
    bindQueue chan (exchangeName exchange) key key
    return $ AMQPConn conn chan exchange queue
  where
    key = queueName queue

disconnect :: AMQPConn -> IO ()
disconnect = closeConnection . amqpConn

openConnection' :: URI -> IO Connection
openConnection' uri =
    openConnection host vhost user password
  where
    vhost = uriPath uri
    host  = fromMaybe "127.0.0.1" $ uriRegName uri
    auth  = fromMaybe "guest:guest" $ uriUserInfo uri
    [user, password] = splitOn ":" auth

publish :: AMQPConn -> BS.ByteString -> IO ()
publish (AMQPConn _ chan exchange queue) payload =
    publishMsg chan (exchangeName exchange) (queueName queue) message
  where
    message = newMsg { msgBody = BL.fromChunks [payload] }
