-module(couchlib_term).

-include_lib("couch/couch_db.hrl").

% TODO - is this module a good idea? It allows direct use of Erlang terms like
% records and what not. Restricting to proplists isn't awful, but I could see
% applications that would want to store terms directly.

-export([ new/1
		  , new/2
		  , id/1
		  , rev/1
		  , term/1
		  , body/1
		  , deleted/1
		  , avp_value/2
		  , avp_optional_value/2
		  , avp_optional_value/3
		  , avp_optional_binary/2
		]).

new(Term) -> new(couch_uuids:random(), Term).

new(Id, Term) ->
    #doc{id=to_bin(Id), body={[Term]}}.

id(#doc{id=Id}) -> Id.

rev(#doc{revs={_, []}}) -> undefined;
rev(#doc{revs={Pos, [RevId|_]}}) ->
    couch_doc:rev_to_str({Pos, RevId}).

term(#doc{body=Body}=Doc) when is_binary(Body) ->
	term(Doc#doc{body=binary_to_term(Body)});
term(#doc{body={[Term]}}) -> Term.

body(#doc{body=Body}) -> Body.


deleted(#doc{deleted=D}) -> D.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L).


%% --------------------------------------------------------------------
%% Function: 	avp_value/2
%% Description: get matching leaf avp value from an avp-list
%% SearchPatt	list of tokens (matching from root of each avp to leaf token)
%% 				avp must exist
%% Returns:		Value
%% --------------------------------------------------------------------
-spec avp_value(maybe_improper_list(),nonempty_maybe_improper_list()) -> [any()].
avp_value(Avps, SearchPattern) ->
	[Value] = avp_optional_value(Avps, SearchPattern),
	Value.

%% --------------------------------------------------------------------
%% Function: 	avp_optional_value/3
%% Description: get matching leaf avp value from an avp-list
%% SearchPatt	list of tokens (matching from root of each avp to leaf token)
%% 				returns default value if avp does not exist
%% Returns:		Value
%% --------------------------------------------------------------------
-spec avp_optional_value(maybe_improper_list(),nonempty_maybe_improper_list(), any()) -> [any()].
avp_optional_value(Avps, SearchPattern, DefaultValue) ->
	case avp_optional_value(Avps, SearchPattern) of
		[]	-> DefaultValue;
		[Value] -> Value
	end.

%% --------------------------------------------------------------------
%% Function: 	get_optional_value/2
%% Description: get matching leaf avp value from an avp-list
%% SearchPatt	list of tokens (matching from root of each avp to leaf token)
%% Returns:		[Value] | []
%% --------------------------------------------------------------------
-spec avp_optional_value(maybe_improper_list(),nonempty_maybe_improper_list()) -> [any()].
avp_optional_value([{AvpTokenName, AvpValue} | RestAvp], [FirstToken | RestSearchPattern]=SearchPattern) ->
	if
		(AvpTokenName == FirstToken)
		  and (length(RestSearchPattern) == 0) ->
			if
				is_integer(AvpValue) -> [AvpValue];
				is_float(AvpValue) -> [AvpValue];
				is_atom(AvpValue) -> [AvpValue];
				true ->
					case AvpValue of
						[{_,_}|_Rest] -> erlang:error(not_a_leaf_value);
						_ -> [AvpValue]
					end
			end;
		(AvpTokenName == FirstToken)
		  and (length(RestSearchPattern) > 0)
		  and (length(AvpValue) > 0)
		  and is_list(AvpValue) ->
			avp_optional_value(AvpValue, RestSearchPattern);
		true ->
			avp_optional_value(RestAvp, SearchPattern)
	end;
avp_optional_value([], [_FirstToken | _RestSearchPattern]) ->
	[];
avp_optional_value([], []) ->
	erlang:error(invalid_pattern);
avp_optional_value(_Avp, []) ->
	erlang:error(invalid_pattern).


%% --------------------------------------------------------------------
%% Function: 	avp_optional_binary/2
%% Description: get matching leaf avp value from an avp-list
%% SearchPatt	list of tokens (matching from root of each avp to leaf token)
%% Returns:		[Value] | []
%% --------------------------------------------------------------------
-spec avp_optional_binary(maybe_improper_list(),nonempty_maybe_improper_list()) -> [any()].
avp_optional_binary([{AvpTokenName, AvpValue} | RestAvp], [FirstToken | RestSearchPattern]=SearchPattern) ->
	if
		(AvpTokenName == FirstToken)
		  and (length(RestSearchPattern) == 0) ->
			if
				is_integer(AvpValue) -> [list_to_binary(integer_to_list(AvpValue))];
				is_float(AvpValue) -> [list_to_binary(float_to_list(AvpValue))];
				is_atom(AvpValue) -> [list_to_binary(atom_to_list(AvpValue))];
				true ->
					case AvpValue of
						[{_,_}|_Rest] -> erlang:error(not_a_leaf_value);
						_ -> [list_to_binary(AvpValue)]
					end
			end;
		(AvpTokenName == FirstToken)
		  and (length(RestSearchPattern) > 0)
		  and (length(AvpValue) > 0)
		  and is_list(AvpValue) ->
			avp_optional_binary(AvpValue, RestSearchPattern);
		true ->
			avp_optional_binary(RestAvp, SearchPattern)
	end;
avp_optional_binary([], [_FirstToken | _RestSearchPattern]) ->
	[];
avp_optional_binary([], []) ->
	erlang:error(invalid_pattern);
avp_optional_binary(_Avp, []) ->
	erlang:error(invalid_pattern).
