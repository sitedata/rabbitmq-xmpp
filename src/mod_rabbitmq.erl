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
%% @doc RabbitMQ gateway module for ejabberd.
%%
%% All of the exposed functions of this module are private to the
%% implementation. See the <a
%% href="overview-summary.html">overview</a> page for more
%% information.

-module(mod_rabbitmq).

-behaviour(gen_server).
-behaviour(gen_mod).

%% API
-export([start_link/2, 
		 start/2, 
		 stop/1,
		 consumer_stopped/2]).

%% gen_server callbacks
-export([init/1, 
		 handle_call/3, 
		 handle_cast/2, 
		 handle_info/2, 
		 terminate/2, 
		 code_change/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("rabbit.hrl").

-define(VHOST, <<"/">>).
-define(XNAME(Name), #resource{virtual_host = ?VHOST, kind = exchange, name = Name}).
-define(QNAME(Name), #resource{virtual_host = ?VHOST, kind = queue, name = Name}).

-record(state, {host,
				server_host}).
-record(rabbitmq_consumer_process, {queue, pid}).

-define(PROCNAME, ejabberd_mod_rabbitmq).
-define(TABLENAME, ?PROCNAME).

-define(RPC_TIMEOUT, 30000).
-define(RABBITMQ_HEARTBEAT, 5000).
-define(ROUND_ATTEMPTS, 5).

start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    ChildSpec = {Proc,
		 {?MODULE, start_link, [Host, Opts]},
		 temporary,
		 1000,
		 worker,
		 [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:call(Proc, stop),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).

consumer_stopped(Host, QNameBin) ->
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
	gen_server:call(Proc, {consumer_stopped, QNameBin} ).

%%
%% gen_server callbacks
%%
init([Host, Opts]) ->
	RabbitNode = get_rabbit_node_config(),
	put(rabbitmq_node, RabbitNode),
	spawn_link( fun() -> ensure_connection_to_rabbitmq(Host, RabbitNode) end),

    mnesia:create_table(rabbitmq_consumer_process,
			[{attributes, record_info(fields, rabbitmq_consumer_process)}]),

    MyHost = gen_mod:get_opt_host(Host, Opts, "rabbitmq.@HOST@"),
    ejabberd_router:register_route(MyHost),

    probe_queues(MyHost),

    {ok, #state{host = MyHost,
				server_host = Host}}.

handle_call({consumer_stopped, QNameBin}, _From, State) ->
	?DEBUG("Consumer for ~p stopped~n", [QNameBin]),
	delete_consumer_process( QNameBin ),
    {reply, ok, State};

handle_call(get_rabbitmq_node, _From, State) ->
    {reply, get(rabbitmq_node), State};
handle_call(stop, _From, State) ->
	?DEBUG("stop , state: ~p~n",[State]),
    {stop, normal, ok, State}.

handle_cast({set_rabbitmq_node, Node}, State) ->
	put(rabbitmq_node, Node),
	broadcast_new_rabbit_node_to_consumer( Node ),
	{noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({route, From, To, Packet}, 
			#state{server_host = Host} = State) ->
    case catch do_route(Host, From, To, Packet) of
		{'EXIT', Reason} ->
			?ERROR_MSG("~p~nwhen processing: ~p",
					   [Reason, {From, To, Packet}]);
		_ ->
			ok
	end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
	?DEBUG("terminate from ~p ~n reason ~p ~n", [State,Reason]),
    ejabberd_router:unregister_route(State#state.host),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% internal functions
%%
get_rabbit_node_config() ->
	case gen_mod:get_module_opt(global, ?MODULE, rabbitmq_node, undefined) of
		undefined ->
			[_NodeName, NodeHost] = string:tokens(atom_to_list(node()), "@"),
			list_to_atom("rabbit@" ++ NodeHost);
		A ->
			A
	end.

set_rabbit_node( Host, Node) ->
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
	gen_server:cast(Proc, {set_rabbitmq_node, Node} ).

broadcast_new_rabbit_node_to_consumer( Node ) ->
	Consumers = mnesia:dirty_select(rabbitmq_consumer_process, [{#rabbitmq_consumer_process{ pid='$1', _ = '_' },[],['$1']}]),
	lists:foreach(fun(Pid) -> mod_rabbitmq_consumer:set_rabbitmq_node(Pid, Node) end, Consumers),
	ok.

%%
%% Heartbeat functions
%%
ensure_connection_to_rabbitmq( Host, RabbitNode ) ->
	timer:sleep(?RABBITMQ_HEARTBEAT),
	case net_adm:ping( RabbitNode ) of
		pong ->
			ensure_connection_to_rabbitmq(Host, RabbitNode);
		pang ->
			ensure_connection_to_rabbitmq(Host, RabbitNode, 1)
	end.
	
ensure_connection_to_rabbitmq( Host, RabbitNode, Times) ->
	timer:sleep(?RABBITMQ_HEARTBEAT),
	case net_adm:ping( RabbitNode ) of
		pong ->
			?INFO_MSG("The connection to rabbitmq node: ~p is ok.",[RabbitNode]),				
			set_rabbit_node( Host, RabbitNode),
			ensure_connection_to_rabbitmq(Host, RabbitNode);
		pang ->
			?WARNING_MSG("Can't connect to rabbitmq node: ~p (~p) times.~n",[RabbitNode,Times]),
			case Times > ?ROUND_ATTEMPTS of
				true ->
					RabbitNode = get_rabbit_node_config(),
					ensure_connection_to_rabbitmq(Host, RabbitNode);
				false ->
					ensure_connection_to_rabbitmq(Host, RabbitNode, Times+1)
			end
	end.

do_route(_Host,#jid{lserver = FromServer} = From,
		 #jid{lserver = ToServer} = To,
		 {xmlelement, "presence", _, _})
  when FromServer == ToServer ->
    %% Break tight loops by ignoring these presence packets.
    ?WARNING_MSG("Tight presence loop between~n~p and~n~p~nbroken.",
		 [From, To]),
    ok;
do_route(_Host, From, #jid{luser = ""} = To, {xmlelement, "presence", _, _} = Packet) ->
    case xml:get_tag_attr_s("type", Packet) of
	"subscribe" ->
	    send_presence(To, From, "unsubscribed");
	"subscribed" ->
	    send_presence(To, From, "unsubscribe"),
	    send_presence(To, From, "unsubscribed");
	"unsubscribe" ->
	    send_presence(To, From, "unsubscribed");

	"probe" ->
	    send_presence(To, From, "");

	_Other ->
	    ?INFO_MSG("Other kind of presence for empty-user JID~n~p", [Packet])
    end,
    ok;
do_route(Host, From, To, {xmlelement, "presence", _, _} = Packet) ->
    QNameBin = jid_to_qname(From),
    {XNameBin, RKBin} = jid_to_xname(To),
    case xml:get_tag_attr_s("type", Packet) of
	"subscribe" ->
			case is_bound( XNameBin ) of
				false ->
					send_presence(To, From, "unsubscribed");
				true ->
					send_presence(To, From, "subscribe")
			end;
	"subscribed" ->
	    case check_and_bind(XNameBin, RKBin, QNameBin) of
		true ->
		    send_presence(To, From, "subscribed"),
		    send_presence(To, From, "");
		false ->
		    send_presence(To, From, "unsubscribed"),
		    send_presence(To, From, "unsubscribe")
	    end;
	"unsubscribe" ->
	    maybe_unsub(From, To, XNameBin, RKBin, QNameBin),
	    send_presence(To, From, "unsubscribed");
	"unsubscribed" ->
	    maybe_unsub(From, To, XNameBin, RKBin, QNameBin);

	"" ->
	    add_consumer_member(Host, QNameBin, From, RKBin, To#jid.lserver, extract_priority(Packet));
	"unavailable" ->
	    remove_consumer_member(QNameBin, From, RKBin, false);

	"probe" ->
	    case is_subscribed(XNameBin, RKBin, QNameBin) of
		true ->
		    send_presence(To, From, "");
		false ->
		    ok
	    end;

	_Other ->
	    ?INFO_MSG("Other kind of presence~n~p", [Packet])
    end,
    ok;
do_route(_Host, From, To, {xmlelement, "message", _, _} = Packet) ->
    case xml:get_subtag_cdata(Packet, "body") of
	"" ->
	    ?DEBUG("Ignoring message with empty body", []);
	Body ->
	    case To#jid.luser of
		"" ->
		    send_command_reply(To, From, do_command(To, From, Body, parse_command(Body)));
		_ ->
		    case xml:get_tag_attr_s("type", Packet) of
			"error" ->
			    ?ERROR_MSG("Received error message~n~p -> ~p~n~p", [From, To, Packet]);
			_ ->
					{XNameBin, RKBin} = jid_to_xname(To),
					mod_rabbitmq_util:publish_message( XNameBin, RKBin, Body)
		    end
	    end
    end,
    ok;
do_route(_Host, From, To, {xmlelement, "iq", _, Els0} = Packet) ->
    Els = xml:remove_cdata(Els0),
    IqId = xml:get_tag_attr_s("id", Packet),
    case xml:get_tag_attr_s("type", Packet) of
	"get" -> reply_iq(To, From, IqId, do_iq(get, From, To, Els));
	"set" -> reply_iq(To, From, IqId, do_iq(set, From, To, Els));
	Other -> ?WARNING_MSG("Unsolicited IQ of type ~p~n~p ->~n~p~n~p",
			      [Other, From, To, Packet])
    end,
    ok;
do_route(_Host, _From, _To, _Packet) ->
    ?INFO_MSG("**** DROPPED~n~p~n~p~n~p", [_From, _To, _Packet]),
    ok.

reply_iq(From, To, IqId, {OkOrError, Els}) ->
    ?DEBUG("IQ reply ~p~n~p ->~n~p~n~p", [IqId, From, To, Els]),
    TypeStr = case OkOrError of
		  ok ->
		      "result";
		  error ->
		      "error"
	      end,
    Attrs = case IqId of
		"" -> [{"type", TypeStr}];
		_ -> [{"type", TypeStr}, {"id", IqId}]
	    end,
    ejabberd_router:route(From, To, {xmlelement, "iq", Attrs, Els}).

do_iq(_GetOrSet, _From, _To, []) ->
    {error, iq_error("modify", "bad-request", "Missing IQ element")};
do_iq(_GetOrSet, _From, _To, [_, _ | _]) ->
    {error, iq_error("modify", "bad-request", "Too many IQ elements")};
do_iq(GetOrSet, From, To, [Elt]) ->
    Xmlns = xml:get_tag_attr_s("xmlns", Elt),
    do_iq1(GetOrSet, From, To, Xmlns, Elt).


do_iq1(get, _From, To, "http://jabber.org/protocol/disco#info",
       {xmlelement, "query", _, _}) ->
    {XNameBin, RKBin} = jid_to_xname(To),
    case XNameBin of
	<<>> -> disco_info_module(To#jid.lserver);
	_ -> disco_info_exchange(XNameBin, RKBin)
    end;
do_iq1(get, _From, To, "http://jabber.org/protocol/disco#items",
       {xmlelement, "query", _, _}) ->
    {XNameBin, RKBin} = jid_to_xname(To),
    case XNameBin of
	<<>> -> disco_items_module(To#jid.lserver);
	_ -> disco_items_exchange(XNameBin, RKBin)
    end;
do_iq1(_GetOrSet, _From, _To, Xmlns, RequestElement) ->
    ?DEBUG("Unimplemented IQ feature ~p~n~p", [Xmlns, RequestElement]),
    {error, iq_error("cancel", "feature-not-implemented", "")}.

disco_info_module(_Server) ->
    {ok, disco_info_result([{"component", "generic", "AMQP router module"},
			    {"component", "router", "AMQP router module"},
			    {"component", "presence", "AMQP router module"},
			    {"client", "bot", "RabbitMQ control interface"},
			    {"gateway", "amqp", "AMQP gateway"},
			    {"hierarchy", "branch", ""},
			    "http://jabber.org/protocol/disco#info",
			    "http://jabber.org/protocol/disco#items"])}.

disco_info_exchange(XNameBin, _RKBin) ->
	?DEBUG("disco_info_exchange: ~p ", [XNameBin]),
    Tail = 	case mod_rabbitmq_util:get_exchange( XNameBin ) of
				undefined ->
					?DEBUG("... not present ~n", []),
					[];
				Exchange = #exchange{type = TypeAtom} ->
					?DEBUG("... exists, exchange: ~p~n", [Exchange]),
					["amqp-exchange-" ++ atom_to_list(TypeAtom)]
			end,
	{ok, disco_info_result([{"component", "bot", "AMQP Exchange"},
							{"hierarchy", "leaf", ""},
							"amqp-exchange"
							| Tail])}.

disco_items_module(Server) ->
    {ok, disco_items_result([jlib:make_jid(XNameStr, Server, "")
			     || XNameStr <- all_exchange_names()])}.

disco_items_exchange(_XNameStr, _RKBin) ->
    {ok, disco_items_result([])}.

disco_info_result(Pieces) ->
    disco_info_result(Pieces, []).

disco_info_result([], ConvertedPieces) ->
    [{xmlelement, "query", [{"xmlns", "http://jabber.org/protocol/disco#info"}], ConvertedPieces}];
disco_info_result([{Category, Type, Name} | Rest], ConvertedPieces) ->
    disco_info_result(Rest, [{xmlelement, "identity", [{"category", Category},
							      {"type", Type},
							      {"name", Name}], []}
				    | ConvertedPieces]);
disco_info_result([Feature | Rest], ConvertedPieces) ->
    disco_info_result(Rest, [{xmlelement, "feature", [{"var", Feature}], []}
				    | ConvertedPieces]).

disco_items_result(Pieces) ->
    [{xmlelement, "query", [{"xmlns", "http://jabber.org/protocol/disco#items"}],
      [{xmlelement, "item", [{"jid", jlib:jid_to_string(Jid)}], []} || Jid <- Pieces]}].

iq_error(TypeStr, ConditionStr, MessageStr) ->
    iq_error(TypeStr, ConditionStr, MessageStr, []).

iq_error(TypeStr, ConditionStr, MessageStr, ExtraElements) ->
    [{xmlelement, "error", [{"type", TypeStr}],
      [{xmlelement, ConditionStr, [], []},
       {xmlelement, "text", [{"xmlns", "urn:ietf:params:xml:ns:xmpp-stanzas"}],
	[{xmlcdata, MessageStr}]}
       | ExtraElements]}].

extract_priority(Packet) ->
    case xml:get_subtag_cdata(Packet, "priority") of
	"" ->
	    0;
	S ->
	    list_to_integer(S)
    end.

jid_to_qname(#jid{luser = U, lserver = S}) ->
    list_to_binary(U ++ "@" ++ S).

jid_to_xname(#jid{luser = U, lresource = R}) ->
    {list_to_binary(U), list_to_binary(R)}.

qname_to_jid(QNameBin) when is_binary(QNameBin) ->
    case jlib:string_to_jid(binary_to_list(QNameBin)) of
	error ->
	    error;
	JID ->
	    case JID#jid.luser of
		"" ->
		    error;
		_ ->
		    JID
	    end
    end.

maybe_unsub(From, To, XNameBin, RKBin, QNameBin) ->
    case is_subscribed(XNameBin, RKBin, QNameBin) of
	true ->
	    do_unsub(From, To, XNameBin, RKBin, QNameBin);
	false ->
	    ok
    end,
    ok.

do_unsub(QJID, XJID, XNameBin, RKBin, QNameBin) ->
    send_presence(XJID, QJID, "unsubscribed"),
    send_presence(XJID, QJID, "unsubscribe"),
    case unbind_and_delete(XNameBin, RKBin, QNameBin) of
	no_subscriptions_left ->
	    remove_consumer_member(QNameBin, QJID, none, true),
	    ok;
	subscriptions_remain ->
	    ok
    end.

get_bound_queues(XNameBin) ->
	Bindings = mod_rabbitmq_util:get_bindings_by_exchange( XNameBin ),

    [{QNameBin, RKBin} ||
	#binding{destination = #resource{name = QNameBin}, key = RKBin} <- Bindings].

unsub_all(XNameBin, ExchangeJID) ->
    {atomic, BindingDescriptions} =
	mnesia:transaction(
	  fun () ->
		  BoundQueues = get_bound_queues(XNameBin),
		  mod_rabbitmq_util:delete_exchange( XNameBin ),
		  BoundQueues
	  end),
    ?INFO_MSG("unsub_all~n~p~n~p~n~p", [XNameBin, ExchangeJID, BindingDescriptions]),
    lists:foreach(fun ({QNameBin, RKBin}) ->
			  case qname_to_jid(QNameBin) of
			      error ->
				  ignore;
			      QJID ->
				  do_unsub(QJID,
					   jlib:jid_replace_resource(ExchangeJID,
								     binary_to_list(RKBin)),
					   XNameBin,
					   RKBin,
					   QNameBin)
			  end
		  end, BindingDescriptions),
    ok.

send_presence(From, To, "") ->
    ?DEBUG("Sending sub reply of type ((available))~n~p -> ~p", [From, To]),
    ejabberd_router:route(From, To, {xmlelement, "presence", [], []});
send_presence(From, To, TypeStr) ->
    ?DEBUG("Sending sub reply of type ~p~n~p -> ~p", [TypeStr, From, To]),
    ejabberd_router:route(From, To, {xmlelement, "presence", [{"type", TypeStr}], []}).

send_message(From, To, TypeStr, BodyStr) ->
    XmlBody = {xmlelement, "message",
	       [{"type", TypeStr},
		{"from", jlib:jid_to_string(From)},
		{"to", jlib:jid_to_string(To)}],
	       [{xmlelement, "body", [],
		 [{xmlcdata, BodyStr}]}]},
    ?DEBUG("Delivering ~p -> ~p~n~p", [From, To, XmlBody]),
    ejabberd_router:route(From, To, XmlBody).

is_subscribed(XNameBin, RKBin, QNameBin) ->
    XName = ?XNAME(XNameBin),
	Bindings = mod_rabbitmq_util:get_bindings_by_queue( QNameBin ),
    lists:any(fun (#binding{source = N, key = R})
                    when N == XName andalso R == RKBin ->
					  true;
				  (_) ->
					  false
			  end, Bindings).

is_bound( XNameBin ) ->
	?DEBUG("is_bound: checking ~p ", [XNameBin]),
	case mod_rabbitmq_util:get_exchange( XNameBin) of
		undefined ->
			?DEBUG("... not bound.~n",[]),
			false;
		_E ->
			?DEBUG("... is bound.~n",[]),
			true
	end.			
							 
check_and_bind(XNameBin, RKBin, QNameBin) ->
    ?DEBUG("Checking ~p ~p ~p", [XNameBin, RKBin, QNameBin]),
	case mod_rabbitmq_util:get_exchange( XNameBin ) of
		undefined ->
			?DEBUG("... not present ~n", [XNameBin]),
			false;
		Exchange ->
			?DEBUG("... exists, exchange: ~p~n", [Exchange]),
			case mod_rabbitmq_util:declare_queue( QNameBin ) of
				{error, Reason} ->
					?ERROR_MSG("check_and_bind: error ~p~n",[Reason]);
				_ ->
					case mod_rabbitmq_util:add_binding( XNameBin, QNameBin, RKBin ) of
						{error, Reason} ->
							?ERROR_MSG("check_and_bind: error ~p~n",[Reason]);
						_ ->
							ok
					end
			end,
			true
    end.

unbind_and_delete(XNameBin, RKBin, QNameBin) ->
    ?DEBUG("Unbinding ~p ~p ~p", [XNameBin, RKBin, QNameBin]),
    case mod_rabbitmq_util:remove_binding( XNameBin, QNameBin, RKBin ) of
	{error, Reason} ->
	    ?DEBUG("... queue or exchange not found: ~p. Ignoring", [Reason]),
	    no_subscriptions_left;
	_ ->
	    ?DEBUG("... checking count of remaining bindings ...", []),
	    %% Obvious (small) window where Problems (races) May Occur here
		Bindings = mod_rabbitmq_util:get_bindings_by_queue( QNameBin ),
	    case length( Bindings ) of
		0 ->
		    ?DEBUG("... and deleting", []),
		    case mod_rabbitmq_util:get_queue( QNameBin ) of
				{ok, Q} -> 
					case mod_rabbitmq_util:delete_queue( Q ) of
						{error, Reason} ->
							?ERROR_MSG("unbind_and_delete: error ~p~n",[Reason]);
						_ ->
							ok
					end;
				{error, Reason} ->
					?ERROR_MSG("unbind_and_delete: error ~p~n",[Reason])
		    end,
		    ?DEBUG("... deletion complete.", []),
		    no_subscriptions_left;
		_PositiveCountOfBindings ->
		    ?DEBUG("... and leaving the queue alone", []),
		    subscriptions_remain
	    end
    end.

all_exchange_names() ->
	Exchanges = mod_rabbitmq_util:all_exchanges(),
    [binary_to_list(XNameBin) ||
		#exchange{name = #resource{name = XNameBin}} <- Exchanges, 
		XNameBin =/= <<>>].

all_exchanges() ->
	Exchanges = mod_rabbitmq_util:all_exchanges(),
    [{binary_to_list(XNameBin),
      TypeAtom,
      case IsDurable of
		  true -> durable;
		  false -> transient
      end,
      Arguments}
	 ||
		#exchange{name = #resource{name = XNameBin},
				  type = TypeAtom,
				  durable = IsDurable,
				  arguments = Arguments}
			<- Exchanges,
	XNameBin =/= <<>>].

probe_queues(Server) ->
	Queues = mod_rabbitmq_util:all_queues(),
    probe_queues(Server, Queues).

probe_queues(_Server, []) ->
    ok;
probe_queues(Server, [#amqqueue{name = #resource{name = QNameBin}} | Rest]) ->
    ?DEBUG("**** Probing ~p", [QNameBin]),
    case qname_to_jid(QNameBin) of
	error ->
	    probe_queues(Server, Rest);
	JID ->
		Bindings = mod_rabbitmq_util:get_bindings_by_queue( QNameBin ),
 	    probe_bindings(Server, JID, Bindings),
	    probe_queues(Server, Rest)
    end;
probe_queues(Server, Arg) ->
	?ERROR_MSG("probe_queues: unknown calling : ~p ~p ~n",[Server, Arg]).

probe_bindings(_Server, _JID, []) ->
    ok;

probe_bindings(Server, JID, [#binding{source = #resource{name = XNameBin}} | Rest]) ->
    ?DEBUG("**** Probing ~p ~p ~p", [JID, XNameBin, Server]),
    SourceJID = jlib:make_jid(binary_to_list(XNameBin), Server, ""),
    send_presence(SourceJID, JID, "probe"),
    send_presence(SourceJID, JID, ""),
    probe_bindings(Server, JID, Rest);

probe_bindings(Server, JID, Arg ) ->
	?ERROR_MSG("probe_bindings: unknown calling : ~p ~p ~n ~p ~n",[Server, JID, Arg]).

add_consumer_member(Host, QNameBin, JID, RKBin, Server, Priority) ->
	Pid = case get_consumer_process( QNameBin ) of
			  undefined ->
				  new_consumer_process( Host, QNameBin, JID, RKBin, Server, Priority );
			  Pid1 ->
				  Pid1
		  end,
	mod_rabbitmq_consumer:add_member(Pid, JID, RKBin, Priority).

remove_consumer_member(QNameBin, JID, RKBin, AllResources) ->
	case get_consumer_process( QNameBin ) of
		undefined ->
			?WARNING_MSG("no consumer process, why remove? ~p~n",
						 [{QNameBin, JID, RKBin, AllResources}]);
		Pid ->
			mod_rabbitmq_consumer:remove_member( Pid, JID, RKBin, AllResources)
	end.

get_consumer_process( QNameBin ) ->
	case mnesia:dirty_read({rabbitmq_consumer_process, QNameBin}) of
		[#rabbitmq_consumer_process{pid = Pid}] ->
			Pid;
		[] ->
			undefined
	end.

new_consumer_process( Host, QNameBin, JID, RKBin, Server, Priority ) ->
	{ok, Pid} = mod_rabbitmq_consumer:start(Host, QNameBin, JID, RKBin, 
											Server, Priority, get(rabbitmq_node)),
	mnesia:transaction( 
	  fun() ->
			  mnesia:write(#rabbitmq_consumer_process{queue = QNameBin, pid = Pid})
	  end),
	Pid.

delete_consumer_process( QNameBin ) ->
	mnesia:transaction( 
	  fun() ->			  
			  mnesia:delete({rabbitmq_consumer_process, QNameBin})
	  end).

parse_command(Str) ->
    [Cmd | Args] = string:tokens(Str, " "),
    {stringprep:tolower(Cmd), Args}.

command_list() ->
    [{"help", "'help (command)'. Provides help for other commands."},
     {"exchange.declare", "'exchange.declare (name) [-type (type)] [-transient]'. Creates a new AMQP exchange."},
     {"exchange.delete", "'exchange.delete (name)'. Deletes an AMQP exchange."},
     {"bind", "'bind (exchange) (jid)'. Binds an exchange to another JID, with an empty routing key. 'bind (exchange) (jid) (key)'. Binds an exchange to another JID, with the given routing key."},
     {"unbind", "'unbind (exchange) (jid)'. Unbinds an exchange from another JID, using an empty routing key. 'unbind (exchange) (jid) (key)'. Unbinds using the given routing key."},
     {"list", "'list'. List available exchanges.\nOR 'list (exchange)'. List subscribers to an exchange."}].

command_names() ->
    [Cmd || {Cmd, _} <- command_list()].

do_command(_To, _From, _RawCommand, {"help", []}) ->
    {ok,
     "Here is a list of commands. Use 'help (command)' to get details on any one.~n~p",
     [command_names()]};
do_command(_To, _From, _RawCommand, {"help", [Cmd | _]}) ->
    case lists:keysearch(stringprep:tolower(Cmd), 1, command_list()) of
	{value, {_, HelpText}} ->
	    {ok, HelpText};
	false ->
	    {ok, "Unknown command ~p. Try plain old 'help'.", [Cmd]}
    end;
do_command(_To, _From, _RawCommand, {"exchange.declare", [NameStr | Args]}) ->
    do_command_declare(NameStr, Args, []);
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"exchange.delete", [NameStr]}) ->
    case NameStr of
	"amq." ++ _ ->
	    {ok, "You are not allowed to delete names starting with 'amq.'."};
	_ ->
	    XNameBin = list_to_binary(NameStr),
	    unsub_all(XNameBin, jlib:make_jid(NameStr, Server, "")),
	    %%rabbit_exchange:delete(?XNAME(XNameBin), false),
	    {ok, "Exchange ~p deleted.", [NameStr]}
    end;
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"bind", [NameStr, JIDStr]}) ->
    do_command_bind(Server, NameStr, JIDStr, "");
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"bind", [NameStr, JIDStr, RK | RKs]}) ->
    do_command_bind(Server, NameStr, JIDStr, check_ichat_brokenness(JIDStr, RK, RKs));
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr]}) ->
    do_command_unbind(Server, NameStr, JIDStr, "");
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr, RK | RKs]}) ->
    do_command_unbind(Server, NameStr, JIDStr, check_ichat_brokenness(JIDStr, RK, RKs));
do_command(_To, _From, _RawCommand, {"list", []}) ->
    {ok, "Exchanges available:~n~p",
     [all_exchanges()]};
do_command(_To, _From, _RawCommand, {"list", [NameStr]}) ->
    {atomic, BindingDescriptions} =
	mnesia:transaction(fun () -> get_bound_queues(list_to_binary(NameStr)) end),
    {ok, "Subscribers to ~p:~n~p",
     [NameStr, [{binary_to_list(QN), binary_to_list(RK)} || {QN, RK} <- BindingDescriptions]]};
do_command(_To, _From, RawCommand, _Parsed) ->
    {error,
     "I am a rabbitmq bot. Your command ~p was not understood.~n" ++
     "Here is a list of commands:~n~p",
     [RawCommand, command_names()]}.

send_command_reply(From, To, {Status, Fmt, Args}) ->
    send_command_reply(From, To, {Status, io_lib:format(Fmt, Args)});
send_command_reply(From, To, {ok, ResponseIoList}) ->
    send_message(From, To, "chat", lists:flatten(ResponseIoList));
send_command_reply(From, To, {error, ResponseIoList}) ->
    send_message(From, To, "chat", lists:flatten(ResponseIoList)).

get_arg(ParsedArgs, Key, DefaultValue) ->
    case lists:keysearch(Key, 1, ParsedArgs) of
	{value, {_, V}} ->
	    V;
	false ->
	    DefaultValue
    end.

do_command_declare(NameStr, ["-type", TypeStr | Rest], ParsedArgs) ->
    do_command_declare(NameStr, Rest, [{type, TypeStr} | ParsedArgs]);
do_command_declare(NameStr, ["-transient" | Rest], ParsedArgs) ->
    do_command_declare(NameStr, Rest, [{durable, false} | ParsedArgs]);
do_command_declare(NameStr, [], ParsedArgs) ->
    case NameStr of
	"amq." ++ _ ->
	    {ok, "Names may not start with 'amq.'."};
	_ ->
			XNameBin = list_to_binary( NameStr ),
			TypeBin = list_to_binary(get_arg(ParsedArgs, type, "fanout")),
			Durable = get_arg(ParsedArgs, durable, true),			
			case mod_rabbitmq_util:declare_exchange( XNameBin, TypeBin, Durable, false ) of
				{error, Reason} ->
					?ERROR_MSG("do_command_declare error ~p~n",[Reason]),
					{error, "Bad exchange type ( or maybe rabbit rpc error )."};
				#exchange{}-> 
					{ok, "Exchange ~p of type ~p declared. Now you can subscribe to it.",
					 [NameStr, TypeBin]}
			end
    end.

interleave(_Spacer, []) ->
    [];
interleave(_Spacer, [E]) ->
    E;
interleave(Spacer, [E | Rest]) ->
    E ++ Spacer ++ interleave(Spacer, Rest).

check_ichat_brokenness(JIDStr, RK, RKs) ->
    IChatLink = "[mailto:" ++ JIDStr ++ "]",
    if
	RK == IChatLink ->
	    interleave(" ", RKs);
	true ->
	    interleave(" ", [RK | RKs])
    end.

do_command_bind(Server, NameStr, JIDStr, RK) ->
    XJID = jlib:make_jid(NameStr, Server, RK),
    QJID = jlib:string_to_jid(JIDStr),
    send_presence(XJID, QJID, "subscribe"),
    {ok, "Subscription process ~p <--> ~p initiated. Good luck!",
     [jlib:jid_to_string(XJID), jlib:jid_to_string(QJID)]}.

do_command_unbind(Server, NameStr, JIDStr, RK) ->
    XJID = jlib:make_jid(NameStr, Server, RK),
    QJID = jlib:string_to_jid(JIDStr),
    do_unsub(QJID, XJID, list_to_binary(NameStr), list_to_binary(RK), list_to_binary(JIDStr)),
    {ok, "Unsubscription process ~p <--> ~p initiated. Good luck!",
     [jlib:jid_to_string(XJID), jlib:jid_to_string(QJID)]}.
