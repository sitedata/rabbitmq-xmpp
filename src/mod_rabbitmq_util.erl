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

-compile(export_all).

%% API
-export([call/3]).
-export([get_binstring_guid/0,
		 basic_consume/2,
		 cancel_consume/2,
		 get_exchange/1]).

-include("ejabberd.hrl").
-include("rabbit.hrl").

-define(VHOST, <<"/">>).
-define(XNAME(Name), #resource{virtual_host = ?VHOST, kind = exchange, name = Name}).
-define(QNAME(Name), #resource{virtual_host = ?VHOST, kind = queue, name = Name}).

call(M, F, A) ->
	rabbit_call(M, F, A).

%%
%% internal functions
%%
basic_consume( QNameBin , ConsumerTag )->
	QName = ?QNAME( QNameBin ),
	{ok, Queue} = get_queue( QName ),
	case rabbit_call(rabbit_amqqueue, basic_consume,
					 [Queue, true, self(), undefined, ConsumerTag, false, undefined])  of
		{error, Reason} ->
			?ERROR_MSG("basic_consume error ~p~n",[Reason]),
			undefined;
		R ->
			?DEBUG("basic_consume return ~p~n",[R]),
			R
	end.
cancel_consume( QNameBin, ConsumerTag ) ->
	QName = ?QNAME( QNameBin ),
	{ok, Queue} = get_queue( QName ),
	case rabbit_call(rabbit_amqqueue, basic_cancel,
					 [Queue, self(), ConsumerTag, undefined]) of
		{error, Reason} ->
			?ERROR_MSG("basic_cancel error ~p~n",[Reason]),
			undefined;
		R ->
			?DEBUG("basic_cancel return ~p~n",[R]),
			R
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

with_queue(QN, Fun) ->
    %% FIXME: No way of using rabbit_amqqueue:with/2, so using this awful kludge :-(
    case mod_rabbitmq_util:call(rabbit_amqqueue, lookup, [QN]) of
        {ok, Q} ->
            Fun(Q);
		{error, Reason} ->
			?ERROR_MSG("mod_rabbitmq_util:call error in ~p~n~p~n",
					   [with_queue, {QN, Reason}])
    end.

get_queue( QName ) ->
	case rabbit_call(rabbit_amqqueue, lookup, [QName]) of
		{error, Reason} ->
			?ERROR_MSG("lookup queue: error ~p~n",[Reason]),
			undefined;
		R ->
			?DEBUG("lookup queue: return ~p~n",[R]),
			R
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
