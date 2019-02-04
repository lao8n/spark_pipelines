import unittest
import pipeline_planner as pp


class PipelinePlannerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipeline_xyz = pp.DfVars({"x": pp.Var(),
                                      "y": pp.Var(),
                                      "z": pp.Var()})
        cls.pipeline_wxyz = pp.DfVars({"w": pp.Var(),
                                       "x": pp.Var(),
                                       "y": pp.Var(),
                                       "z": pp.Var()})

    def reset(self):
        self.pipeline_xyz = pp.DfVars({"x": pp.Var(),
                                       "y": pp.Var(),
                                       "z": pp.Var()})
        self.pipeline_wxyz = pp.DfVars({"w": pp.Var(),
                                        "x": pp.Var(),
                                        "y": pp.Var(),
                                        "z": pp.Var()})

    def test_add_col(self):

        # Data = "xyz"
        # add_col = "w"
        self.reset()

        print("DATASET: xyz")
        self.pipeline_xyz.print_vars()

        flag = self.pipeline_xyz.add_col("w")

        # Test add_col returns True
        self.assertEqual(flag,
                         True,
                         "add_col(w) on pipeline_xyz returns True")

        # Check w
        w_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["w"],
                         w_var,
                         "add_col(w) on pipeline_xyz w variable")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["x"],
                         x_var,
                         "add_col(w) on pipeline_xyz x variable")

        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["y"],
                         y_var,
                         "add_col(w) on pipeline_xyz y variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["z"],
                         z_var,
                         "add_col(w) on pipeline_xyz z variable")

    def test_group_by_agg(self):

        # Data = "xyz"
        # group_by_vars = "x"
        # agg_var = "z"
        self.reset()

        print("DATASET: xyz")
        self.pipeline_xyz.print_vars()

        flag = self.pipeline_xyz.group_by_agg(group_by_vars=["x"],
                                              agg_var="z")

        self.assertEqual(flag,
                         True,
                         "group_by_agg([x],z) on pipeline_xyz returns True")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["x"],
                         x_var,
                         "group_by_agg([x],z) on pipeline_xyz x variable")

        # Check y
        y_var = pp.Var(exists_flag=False,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["y"],
                         y_var,
                         "group_by_agg([x],z) on pipeline_xyz y variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=True)

        # Data = "xyz"
        # group_by_vars = "x", "y"
        # agg_var = "z"
        self.reset()

        print("DATASET: xyz")
        self.pipeline_xyz.print_vars()

        flag = self.pipeline_xyz.group_by_agg(group_by_vars=["x", "y"],
                                              agg_var="z")

        self.assertEqual(flag,
                         True,
                         "group_by_agg([x,y],z) on pipeline_xyz returns True")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["x"],
                         x_var,
                         "group_by_agg([x,y],z) on pipeline_xyz x variable")

        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["y"],
                         y_var,
                         "group_by_agg([x,y],z) on pipeline_xyz y variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["z"],
                         z_var,
                         "group_by_agg([x,y],z) on pipeline_xyz z variable")

    def test_group_by_melt(self):

        # Data = "xyz"
        # group_by_vars = "x"
        # melt_var = "y"
        # cross_var = "z"
        self.reset()

        print("DATASET: xyz")
        # Need to first pivot data so have something to melt on
        self.pipeline_xyz.group_by_pivot(group_by_vars=["x"],
                                         pivot_var="y",
                                         agg_var="z")

        flag = self.pipeline_xyz.group_by_melt(id_vars=["x"],
                                               melt_var="y",
                                               cross_var="z")

        # Test group_by_melt returns True
        self.assertEqual(flag,
                         True,
                         "group_by_melt([x],y,z) on pipeline_xyz returns True")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["x"],
                         x_var,
                         "group_by_melt([x],y,z) on pipeline_xyz x variable")

        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["y"],
                         y_var,
                         "group_by_melt([x],y,z) on pipeline_xyz y variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["z"],
                         z_var,
                         "group_by_melt([x],y,z) on pipeline_xyz z variable")

        # Data = "wxyz"
        # group_by_vars = "x"
        # melt_var = "y"
        # cross_var = "z"
        self.reset()

        print("DATASET: wxyz")
        # Need to first pivot data so have something to melt on
        self.pipeline_wxyz.group_by_pivot(group_by_vars=["x"],
                                          pivot_var="y",
                                          agg_var="z")

        flag = self.pipeline_wxyz.group_by_melt(group_by_vars=["x"],
                                                melt_var="y",
                                                cross_var="z")

        # Test group_by_pivot returns True
        self.assertEqual(flag,
                         True,
                         "group_by_melt([x],y,z) on pipeline_wxyz returns "
                         "True")

        # Check w
        w_var = pp.Var(exists_flag=False,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["w"],
                         w_var,
                         "group_by_melt([x],y,z) on pipeline_wxyz w variable")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["x"],
                         x_var,
                         "group_by_melt([w,x],y,z) on pipeline_wxyz x "
                         "variable")

        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["y"],
                         y_var,
                         "group_by_melt([w,x],y,z) on pipeline_wxyz y "
                         "variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=True)

        self.assertEqual(self.pipeline_wxyz.vars["z"],
                         z_var,
                         "group_by_melt([w,x],y,z) on pipeline_wxyz z "
                         "variable")





    def test_group_by_pivot(self):

        # Data = "xyz"
        # group_by_vars = "x"
        # pivot_var = "y"
        # agg_var = "z"
        self.reset()

        print("DATASET: xyz")
        self.pipeline_xyz.print_vars()

        flag = self.pipeline_xyz.group_by_pivot(group_by_vars=["x"],
                                                pivot_var="y",
                                                agg_var="z")

        # Test group_by_pivot returns True
        self.assertEqual(flag,
                         True,
                         "group_by_pivot([x],y,z) on pipeline_xyz returns True")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_xyz.vars["x"],
                         x_var,
                         "group_by_pivot([x],y,z) on pipeline_xyz x variable")
        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="row",
                       grouped_flag=True,
                       agg_flag=False)
        self.assertEqual(self.pipeline_xyz.vars["y"],
                         y_var,
                         "group_by_pivot([x],y,z) on pipeline_xyz y variable")
        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="cross",
                       grouped_flag=False)
        self.assertEqual(self.pipeline_xyz.vars["z"],
                         z_var,
                         "group_by_pivot([x],y,z) on pipeline_xyz z variable")

        # Data = "wxyz"
        # group_by_vars = "x"
        # pivot_var = "y"
        # agg_var = "z"
        self.reset()

        print("DATASET: wxyz")
        self.pipeline_wxyz.print_vars()

        flag = self.pipeline_wxyz.group_by_pivot(group_by_vars=["x"],
                                                 pivot_var="y",
                                                 agg_var="z")

        # Test group_by_pivot returns True
        self.assertEqual(flag,
                         True,
                         "group_by_pivot([x],y,z) on pipeline_wxyz returns "
                         "True")

        # Check w
        w_var = pp.Var(exists_flag=False,
                       type_flag="col",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["w"],
                         w_var,
                         "group_by_pivot([x],y,z) on pipeline_wxyz w variable")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["x"],
                         x_var,
                         "group_by_pivot([x],y,z) on pipeline_wxyz x variable")

        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="row",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["y"],
                         y_var,
                         "group_by_pivot([x],y,z) on pipeline_wxyz y variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="cross",
                       grouped_flag=False,
                       agg_flag=True)

        self.assertEqual(self.pipeline_wxyz.vars["z"],
                         z_var,
                         "group_by_pivot([x],y,z) on pipeline_wxyz z variable")

        # Data = "wxyz"
        # group_by_vars = "w", "x"
        # pivot_var = "y"
        # agg_var = "z"
        self.reset()

        print("DATASET: wxyz")
        self.pipeline_wxyz.print_vars()

        flag = self.pipeline_wxyz.group_by_pivot(group_by_vars=["w", "x"],
                                                 pivot_var="y",
                                                 agg_var="z")

        # Test group_by_pivot returns True
        self.assertEqual(flag,
                         True,
                         "group_by_pivot([w,x],y,z) on pipeline_wxyz returns "
                         "True")

        # Check w
        w_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["w"],
                         w_var,
                         "group_by_pivot([w,x],y,z) on pipeline_wxyz w "
                         "variable")

        # Check x
        x_var = pp.Var(exists_flag=True,
                       type_flag="col",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["x"],
                         x_var,
                         "group_by_pivot([w,x],y,z) on pipeline_wxyz x "
                         "variable")

        # Check y
        y_var = pp.Var(exists_flag=True,
                       type_flag="row",
                       grouped_flag=True,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["y"],
                         y_var,
                         "group_by_pivot([w,x],y,z) on pipeline_wxyz y "
                         "variable")

        # Check z
        z_var = pp.Var(exists_flag=True,
                       type_flag="cross",
                       grouped_flag=False,
                       agg_flag=False)

        self.assertEqual(self.pipeline_wxyz.vars["z"],
                         z_var,
                         "group_by_pivot([w,x],y,z) on pipeline_wxyz z "
                         "variable")


def shape_test_suite():
    shape_suite = unittest.TestSuite()
    shape_suite.addTest(PipelinePlannerTestCase('test_add_col'))
    shape_suite.addTest(PipelinePlannerTestCase('test_group_by_agg'))
    shape_suite.addTest(PipelinePlannerTestCase('test_group_by_pivot'))
    shape_suite.addTest(PipelinePlannerTestCase('test_group_by_melt'))
    return shape_suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(shape_test_suite())
