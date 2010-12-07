%% RabbitMQ gateway module for ejabberd.
%% Based on ejabberd's mod_echo.erl
%%---------------------------------------------------------------------------
%% @author Tony Garnock-Jones <tonyg@lshift.net>
%% @author Rabbit Technologies Ltd. <info@rabbitmq.com>
%% @author LShift Ltd. <query@lshift.net>
%% @copyright 2008 Tony Garnock-Jones and Rabbit Technologies Ltd.; Copyright © 2008-2009 Tony Garnock-Jones and LShift Ltd.
%% @license
%%
%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%% General Public License for more details.
%%                         
%% You should have received a copy of the GNU General Public License
%% along with this program; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
%% 02111-1307 USA
%%---------------------------------------------------------------------------
%%
%% @doc RabbitMQ utility module for ejabberd.
%%
%% All of the exposed functions of this module are private to the
%% implementation. See the <a
%% href="overview-summary.html">overview</a> page for more
%% information.

-module(mod_rabbitmq_util).

%% API
-export([call/3]).
-export([get_binstring_guid/0,
		 basic_consume/2, cancel_consume/2,
		 declare_queue/1, 
		 get_queue/1, all_queues/0, delete_queue/1,
		 declare_exchange/4, is_exchange_exist/1,
		 get_exchange/1, all_exchanges/0, delete_exchange/1,
		 get_bindings_by_exchange/1, get_bindings_by_queue/1,
		 add_binding/3, remove_binding/3,
		 publish_message/3]).

-include("ejabberd.hrl").
-include("rabbit.hrl").

-define(VHOST, <<"/">>).
-define(XNAME(Name), #resource{virtual_host = ?VHOST, kind = exchange, name = Name}).
-define(QNAME(Name), #resource{virtual_host = ?VHOST, kind = queue, name = Name}).

call(M, F, A) ->
	rabbit_call(M, F, A).

basic_consume( QNameBin , ConsumerTag )->
	case get_queue( QNameBin ) of		
		{ok, Queue} ->
			case rabbit_call(rabbit_amqqueue, basic_consume,
							 [Queue, true, self(), undefined, ConsumerTag, false, undefined])  of
				{error, Reason} ->
					?ERROR_MSG("basic_consume error ~p~n",[Reason]),
					{error, 'error_in_basic_consume'};
				R ->
					?DEBUG("basic_consume return ~p~n",[R]),
					R
			end;
		Err ->
			Err
	end.

cancel_consume( QNameBin, ConsumerTag ) ->
	case get_queue( QNameBin ) of
		{ok, Queue} ->
			case rabbit_call(rabbit_amqqueue, basic_cancel,
							 [Queue, self(), ConsumerTag, undefined]) of
				{error, Reason} ->
					?ERROR_MSG("basic_cancel error ~p~n",[Reason]),
					{error, 'error_in_cancel_consume'};
				R ->
					?DEBUG("basic_cancel return ~p~n",[R]),
					R
			end;
		Err ->
			Err
	end.

get_binstring_guid() ->
	case rabbit_call(rabbit_guid, binstring_guid, ["amq.xmpp"]) of
		{error, Reason} ->
			?ERROR_MSG("get_binstring_guid error, reason:~p~n", [Reason]),
			undefined;
		R ->
			?DEBUG("get_binstring_guid return ~p~n",[R]),
			R
	end.

declare_queue( QNameBin ) ->
	QName = ?QNAME( QNameBin ),
	case rabbit_call(rabbit_amqqueue, declare,[QName, true, false, [], none]) of
		{error, Reason} ->
			?ERROR_MSG("declare queue: error ~p~n",[Reason]),
			{error, 'error_in_declare_queue'};
		R ->
			?DEBUG("declare queue: return ~p~n",[R]),
			R
	end.
	
get_queue( QNameBin ) ->
	QName = ?QNAME( QNameBin ),
	case rabbit_call(rabbit_amqqueue, lookup, [QName]) of
		{error, Reason} ->
			?ERROR_MSG("lookup queue: error ~p~n",[Reason]),
			undefined;
		R ->
			?DEBUG("lookup queue: return ~p~n",[R]),
			R
	end.

all_queues() ->
	case rabbit_call(rabbit_amqqueue, list, [?VHOST]) of
		{error, Reason} ->
			?ERROR_MSG("all_queues: error in ~p~n",[Reason]),
			[];
		R ->
			?DEBUG("mod_rabbitmq_util:call in ~p return ~p~n",[rabbit_amqqueues, R]),
			R
	end.

delete_queue( Queue ) ->
	case rabbit_call(rabbit_amqqueue, delete, [Queue, false, false]) of
		{error, Reason} ->
			?ERROR_MSG("delete_queue: error ~p ~n",[Reason]),
			{error, 'error_in_delete_queue'};
		ok ->
			?DEBUG("delete_queue, ok~n",[]),
			ok
	end.

declare_exchange( XNameBin, TypeBin, Durable, AutoDelete ) ->
	case check_exchange_type( TypeBin ) of
		Err = {error, Reason } -> 
			?ERROR_MSG("declare_exchange: error ~p~n",[Reason]),
			Err;
		TypeAtom ->
			XName = ?XNAME(XNameBin),
			case rabbit_call(rabbit_exchange, declare,
							 [XName,TypeAtom,Durable, AutoDelete,[]]) of
				{error, Reason } ->
					?ERROR_MSG("declare_exchange: error ~p~n",[Reason]),
					{error, 'error_in_declare_exchange'};					
				R ->
					R
			end
	end.

is_exchange_exist( XNameBin ) ->
	case get_exchange( XNameBin ) of
		undefined ->
			false;
		_ ->
			true
	end.

get_exchange( XNameBin ) ->
	XName = ?XNAME( XNameBin ),
	case rabbit_call(rabbit_exchange, lookup, [XName]) of
		{error, Reason} ->
			?ERROR_MSG("lookup exchange: error ~p~n",[Reason]),
			undefined;
		R ->
			?DEBUG("lookup exchange: return ~p~n",[R]),
			R
	end.

all_exchanges() ->
	case mod_rabbitmq_util:call(rabbit_exchange, list, [?VHOST]) of
		{error, Reason} ->
			?ERROR_MSG("all_exchanges: error in ~p~n",[Reason]),
			[];
		R ->
			?DEBUG("all_exchanges: return ~p~n",[R]),
			R
	end.

delete_exchange( XNameBin ) ->
	XName = ?XNAME( XNameBin ),
	case rabbit_call(rabbit_exchange, delete, [XName, false]) of
		{error, Reason} ->
			?ERROR_MSG("delete_exchange: error ~p ~n",[Reason]),
			{error, 'error_in_delete_exchange'};
		ok ->
			?DEBUG("delete_exchange, ok~n",[]),
			ok
	end.

get_bindings_by_exchange( XNameBin ) ->	
	XName = ?XNAME(XNameBin),

	case rabbit_call(rabbit_binding, list_for_source, [XName]) of
		{error, Reason} ->
			?ERROR_MSG("get_bindings_by_source: error in ~p~n~p~n",[Reason]),
			[];
		R ->
			?DEBUG("get_bindings_by_source: return ~p~n",[R]),
			R
	end.

get_bindings_by_queue( QNameBin ) ->
	QName = ?QNAME(QNameBin),
	case rabbit_call(rabbit_binding, list_for_destination, [QName]) of
		{error, Reason} ->
			?ERROR_MSG("get_bindings_by_destination: error in ~p~n~p~n",[Reason]),
			[];
		R ->
			?DEBUG("get_bindings_by_destination: return ~p~n",[R]),
			R
	end.

add_binding( XNameBin, QNameBin, RKBin ) ->
	XName = ?XNAME(XNameBin),
	QName = ?QNAME(QNameBin),
	Binding = #binding{source = XName, destination = QName, key = RKBin, args = []},
	case rabbit_call(rabbit_binding, add, [ Binding ]) of
		{error, Reason} ->
			?ERROR_MSG("add_binding: error ~p~n",[Reason]),
			{error, 'error_in_add_binding'};
		R ->
			?DEBUG("add_binding: return ~p~n",[R]),
			R
	end.


remove_binding( XNameBin, QNameBin, RKBin ) ->
	XName = ?XNAME(XNameBin),
	QName = ?QNAME(QNameBin),
	Binding = #binding{source = XName, destination = QName, key = RKBin, args = []},
	case rabbit_call(rabbit_binding, remove, [ Binding ]) of
		{error, Reason} ->
			?ERROR_MSG("remove_binding: error ~p~n",[Reason]),
			{error, 'error_in_remove_binding'};
		R ->
			?DEBUG("remove_binding: return ~p~n",[R]),
			R
	end.
	
publish_message( XNameBin, RKBin, MsgBody ) ->
	%% FIXME: So many roundtrips!!
	XName = ?XNAME(XNameBin),
	MsgBodyBin = list_to_binary( MsgBody ),
	Msg = rabbit_call(rabbit_basic, message,
					  [ XName,RKBin,[{'content_type', <<"text/plain">>}], MsgBodyBin ]),
	Delivery = rabbit_call(rabbit_basic, delivery,[false, false, none, Msg]),
	rabbit_call(rabbit_basic, publish, [Delivery]).

%%
%% internal functions
%%
check_exchange_type( TypeBin ) ->
	case rabbit_call(rabbit_exchange, check_type,[TypeBin]) of
		Err = {error, Reason} ->
			?ERROR_MSG("check_declare_type: error ~p~n",[Reason]),
			Err;
		TypeAtom ->
			TypeAtom
	end.

rabbit_call(M, F, A) ->
	%% FIXME: why use rabbitmq_node?
	Node = get(rabbitmq_node),
	?DEBUG("rabbit_call in ~p: ~p ~p ~p ~p~n",[?MODULE, Node, M, F, A]),   
    case rpc:call(Node, M, F, A) of
        {badrpc, {'EXIT', Reason}} ->
			?ERROR_MSG("rabbit_call error ~p~nwhen processing: ~p",
					   [Reason, {M, F, A}]),
			{error, Reason};
		{badrpc, Reason} ->
			?ERROR_MSG("rabbit_call error ~p~nwhen processing: ~p",
					   [Reason, {M, F, A}]),
			{error, Reason};
        V ->
            V
    end.
