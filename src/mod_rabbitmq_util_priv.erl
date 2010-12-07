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
%% @doc RabbitMQ utility private module for ejabberd.
%%
%% All of the exposed functions of this module are private to the
%% utility module, which are used by RabbitMQ server. See the <a
%% href="overview-summary.html">overview</a> page for more
%% information.

-module(mod_rabbitmq_util_priv).

%% API
-export([publish_message/3]).

publish_message( XName, RKBin, MsgBodyBin ) ->
	Msg = rabbit_basic:message(XName, RKBin, [{'content_type', <<"text/plain">>}], MsgBodyBin ),
	Delivery = rabbit_basic:delivery(false, false, none, Msg),		
	rabbit_basic:publish( Delivery ).
