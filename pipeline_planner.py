class Var:
    def __init__(self, exists_flag=True, type_flag="col", grouped_flag=False,
                 agg_flag=False):
        self.exists_flag = exists_flag
        self.type_flag = type_flag
        self.grouped_flag = grouped_flag
        self.agg_flag = agg_flag

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class DfVars:
    def __init__(self, vars):
        self.vars = vars

    def add_col(self, var_name):

        print("Method: add_col")
        print(" new columns:", var_name)
        if var_name in self.vars:
            return False
        else:
            # Note we force the user to add a var that exists, is of type column
            # is not grouped, and is not aggregated.
            self.vars.update({var_name: Var()})
            self.print_vars()
            return True

    def group_by_agg(self, group_by_vars, agg_var):
        orig_vars = self.vars

        flag_group = self._group_by(group_by_vars)
        flag_agg = self._agg(group_by_vars, agg_var)
        self._drop_vars(group_by_vars + [agg_var])

        print(flag_group, flag_agg)
        if flag_group and flag_agg:
            print("METHOD: group_by_agg(", group_by_vars, ", ", agg_var, ")")
            self.print_vars()
            return True
        else:
            self.vars = orig_vars
            return False

    def group_by_melt(self, id_vars, melt_var, cross_var):
        orig_vars = self.vars


        flag_melt = self._melt_cross(id_vars, melt_var, cross_var)
        self._drop_vars(id_vars + [melt_var] + [cross_var])

        if flag_group and flag_melt:
            print("METHOD: group_by_melt(", group_by_vars, ", ", melt_var,
                  ", ", cross_var, ")")
            self.print_vars()
            return True
        else:
            self.vars = orig_vars
            return False

    def group_by_pivot(self, group_by_vars, pivot_var, agg_var):
        orig_vars = self.vars

        flag_group = self._group_by(group_by_vars)
        flag_pivot = self._pivot(pivot_var)
        flag_pivot_agg = self._pivot_agg(group_by_vars, pivot_var, agg_var)
        self._drop_vars(group_by_vars + [pivot_var] + [agg_var])

        if flag_group and flag_pivot and flag_pivot_agg:
            print("METHOD: group_by_pivot(", group_by_vars, ", ", pivot_var,
                  ", ", agg_var, ")")
            self.print_vars()
            return True
        else:
            self.vars = orig_vars
            return False

    def _agg(self, group_by_vars, agg_var):
        if len(agg_var) != 1:
            return False
        if agg_var not in self.vars:
            return False
        if len(self.vars) - len(group_by_vars) - len(agg_var) > 0:
            self.vars[agg_var].agg_flag = True
        return True

    def _drop_vars(self, selected_vars):
        for var in self.vars:
            if var not in selected_vars:
                self.vars[var].exists_flag = False

    def _group_by(self, group_by_vars):
        # check group_by_vars list has at least one variable to group by
        if len(group_by_vars) < 1:
            raise ValueError("Need at least one group_by_var")
        # group_by_vars become grouped
        for group_by_var in group_by_vars:
            # check group_by_vars variables are all in self.vars
            if group_by_var not in self.vars:
                return False
            # check type col
            if self.vars[group_by_var].type_flag == "col":
                self.vars[group_by_var].grouped_flag = True
            else:
                return False
        return True

    def _melt_cross(self, group_by_vars, melt_var, cross_var):

        if len(cross_var) != 1:
            return False
        if cross_var not in self.vars:
            return False
        if self.vars[cross_var].type_flag == "cross":
            self.vars[cross_var].type_flag = "col"
        else:
            return False
        if self.vars[melt_var].type_flag == "row":
            self.vars[melt_var].type_flag = "col"
        else:
            return False
        if len(self.vars) - len(group_by_vars) - len(melt_var) - len(cross_var) \
                > 0:
            self.vars[cross_var].cross_flag = True
        return True

    def _pivot(self, pivot_var):
        # check there is exactly one pivot_var
        if len(pivot_var) != 1:
            return False
        # check pivot_var in self.vars
        if pivot_var not in self.vars:
            return False
        # check type col
        if self.vars[pivot_var].type_flag == "col":
            self.vars[pivot_var].type_flag = "row"
            self.vars[pivot_var].grouped_flag = True
        else:
            return False
        return True

    def _pivot_agg(self, group_by_vars, pivot_var, agg_var):
        if len(agg_var) != 1:
            return False
        if agg_var not in self.vars:
            return False
        if self.vars[agg_var].type_flag == "col":
            self.vars[agg_var].type_flag = "cross"
        else:
            return False
        if len(self.vars) - len(group_by_vars) - len(pivot_var) - len(agg_var) \
                > 0:
            self.vars[agg_var].agg_flag = True
        return True

    def print_vars(self):
        print("__________________________________________")
        print("| Name | Exists | Type | Grouped | Agg   |")
        print("__________________________________________")
        for var in self.vars:
            print("|", var, "   |", end="")
            if self.vars[var].exists_flag:
                print(" True   |", end="")
            else:
                print(" False  |", end="")
            if self.vars[var].type_flag == "col":
                print(" Col  |", end="")
            elif self.vars[var].type_flag == "row":
                print(" Row  |", end="")
            elif self.vars[var].type_flag == "cross":
                print(" X    |", end="")
            else:
                print("NA    |", end="")
            if self.vars[var].grouped_flag:
                print(" True    |", end="")
            else:
                print(" False   |", end="")
            if self.vars[var].agg_flag:
                print(" True  |")
            else:
                print(" False |")
        print("__________________________________________")
        print("")

    def _ungroup_by(self, ungroup_by_vars):
        # check group_by_vars list has at least one variable to ungroup by
        if len(ungroup_by_vars) < 1:
            raise ValueError("Need at least one ungroup_by_var")
        # ungroup_by_vars that are grouped become ungrouped
        for ungroup_by_var in ungroup_by_vars:
            # check group_by_vars variables are all in self.vars
            if group_by_var not in self.vars:
                return False
            # check type col
            if self.vars[group_by_var].type_flag == "col":
                self.vars[group_by_var].grouped_flag = True
            else:
                return False
        return True
